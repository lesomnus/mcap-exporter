package tail

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"golang.org/x/sync/errgroup"
)

// TailMany tails every MCAP file reachable from paths and forwards all messages
// on out. A path that is a file is tailed directly; a path that is a directory
// is scanned for *.mcap files and watched so newly created split files are
// picked up. It returns when ctx is cancelled (and any directory watchers stop)
// or when all files are finalized and no directories are being watched.
//
// onCaughtUp, if non-nil, is called exactly once when every file present at
// startup has been read up to its current tail (so the latest observed message
// time is now known). Files discovered later are live data and do not affect
// it; if there are no initial files, it fires immediately.
func TailMany(ctx context.Context, paths []string, headers map[string]bool, poll time.Duration, out chan<- Msg, onCaughtUp func()) error {
	eg, ctx := errgroup.WithContext(ctx)

	// Discover the initial file set up front so catch-up can be tracked.
	var (
		initial []string
		dirs    []string
		seen    = map[string]bool{}
	)
	addInitial := func(p string) {
		if strings.HasSuffix(p, ".mcap") && !seen[p] {
			seen[p] = true
			initial = append(initial, p)
		}
	}
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			addInitial(p)
			continue
		}
		dirs = append(dirs, p)
		entries, err := filepath.Glob(filepath.Join(p, "*.mcap"))
		if err != nil {
			return err
		}
		for _, e := range entries {
			addInitial(e)
		}
	}

	// Catch-up bookkeeping: fire onCaughtUp once all initial files go idle.
	var fireOnce sync.Once
	fire := func() {
		if onCaughtUp != nil {
			fireOnce.Do(onCaughtUp)
		}
	}
	var idleCount atomic.Int32
	total := int32(len(initial))
	onIdle := func() {
		if idleCount.Add(1) >= total {
			fire()
		}
	}
	if total == 0 {
		fire()
	}

	var (
		mu      sync.Mutex
		started = map[string]bool{}
	)
	start := func(p string, idle func()) {
		if !strings.HasSuffix(p, ".mcap") {
			return
		}
		mu.Lock()
		if started[p] {
			mu.Unlock()
			return
		}
		started[p] = true
		mu.Unlock()

		eg.Go(func() error {
			return TailOne(ctx, p, idle, headers, poll, out)
		})
	}

	for _, p := range initial {
		start(p, onIdle)
	}
	for _, dir := range dirs {
		w, err := fsnotify.NewWatcher()
		if err != nil {
			return err
		}
		if err := w.Add(dir); err != nil {
			w.Close()
			return err
		}
		eg.Go(func() error {
			defer w.Close()
			for {
				select {
				case <-ctx.Done():
					return nil
				case ev, ok := <-w.Events:
					if !ok {
						return nil
					}
					if ev.Op&fsnotify.Create != 0 {
						start(ev.Name, nil) // new split: live data, not part of catch-up
					}
				case _, ok := <-w.Errors:
					if !ok {
						return nil
					}
				}
			}
		})
	}

	return eg.Wait()
}
