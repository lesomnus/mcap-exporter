package tail

import (
	"context"
	"io"
	"os"
	"time"
)

// tailReader adapts an *os.File into an io.Reader that does not surface io.EOF
// while the file may still grow. At the current end of file it blocks until a
// wake signal (a filesystem write event) or a poll timeout, then retries. It
// returns io.EOF only after the context is cancelled, so a streaming consumer
// (the MCAP lexer) terminates cleanly on shutdown rather than erroring partway
// through the stream.
//
// It never returns (0, nil): io.ReadFull treats that as a retry and would busy
// loop, so a zero-length read is absorbed by the internal wait instead.
type tailReader struct {
	ctx  context.Context
	f    *os.File
	wake <-chan struct{}
	poll time.Duration

	// onEOF is called the first time the reader reaches the current end of the
	// file and is about to wait for more bytes — i.e. it has caught up to the
	// live tail. It is invoked at most once.
	onEOF    func()
	caughtUp bool
}

func (t *tailReader) Read(p []byte) (int, error) {
	for {
		n, err := t.f.Read(p)
		if n > 0 {
			return n, nil
		}
		if err != nil && err != io.EOF {
			return n, err
		}
		if !t.caughtUp {
			t.caughtUp = true
			if t.onEOF != nil {
				t.onEOF()
			}
		}
		select {
		case <-t.ctx.Done():
			return 0, io.EOF
		case <-t.wake:
		case <-time.After(t.poll):
		}
	}
}
