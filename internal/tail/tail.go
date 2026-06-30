// Package tail streams messages from MCAP files that may still be growing, such
// as a bag being written by `ros2 bag record`. It uses the forward-only MCAP
// lexer (the indexed reader needs a footer/summary that a live file lacks) and
// reduces each message to the topic, schema, and timestamp the rate aggregator
// needs — file names are intentionally dropped so they never reach the metric.
package tail

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/fsnotify/fsnotify"
)

// Msg is a single observed message reduced to what the aggregator needs.
type Msg struct {
	Topic  string
	Schema string
	Time   time.Time
}

type chanInfo struct {
	topic    string
	schemaID uint16
}

// TailOne streams path with the MCAP lexer, forwarding every message on out
// until the file is finalized (a footer is read, i.e. the writer closed or
// rotated the split) or ctx is cancelled. It always reads from the start so
// that the schema/channel records needed to resolve topics are seen; emission
// filtering (e.g. ignoring historical backlog) is the aggregator's job.
//
// idle, if non-nil, is called exactly once when the reader first catches up to
// the live tail (reaches EOF) or when the file is finalized — whichever comes
// first. It lets callers learn when the backlog has been fully read.
func TailOne(ctx context.Context, path string, idle func(), headers map[string]bool, poll time.Duration, out chan<- Msg) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var idleOnce sync.Once
	fireIdle := func() {
		if idle != nil {
			idleOnce.Do(idle)
		}
	}
	defer fireIdle() // a fully-read, finalized file also counts as caught up

	wake := make(chan struct{}, 1)
	stopWatch := watchWrites(ctx, path, wake)
	defer stopWatch()

	tr := &tailReader{ctx: ctx, f: f, wake: wake, poll: poll, onEOF: fireIdle}
	lex, err := mcap.NewLexer(tr)
	if err != nil {
		return err
	}

	var (
		chans   = map[uint16]chanInfo{}
		schemas = map[uint16]string{}
		buf     = make([]byte, 4*1024*1024)
	)
	for {
		tok, rec, err := lex.Next(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		switch tok {
		case mcap.TokenSchema:
			s, err := mcap.ParseSchema(rec)
			if err != nil {
				return err
			}
			schemas[s.ID] = s.Name

		case mcap.TokenChannel:
			c, err := mcap.ParseChannel(rec)
			if err != nil {
				return err
			}
			chans[c.ID] = chanInfo{topic: c.Topic, schemaID: c.SchemaID}

		case mcap.TokenMessage:
			m, err := mcap.ParseMessage(rec)
			if err != nil {
				return err
			}
			ci := chans[m.ChannelID]
			msg := Msg{
				Topic:  ci.topic,
				Schema: schemas[ci.schemaID],
				Time:   resolveTime(headers[ci.topic], m),
			}
			select {
			case out <- msg:
			case <-ctx.Done():
				return nil
			}

		case mcap.TokenFooter:
			// The file has been finalized (closed or rotated to a new split);
			// no further messages will be appended here.
			return nil
		}
	}
}

// watchWrites nudges wake on every filesystem write to path so the tailReader
// can resume promptly instead of waiting out its poll interval. A failure to
// set up the watch degrades to poll-only and is not fatal.
func watchWrites(ctx context.Context, path string, wake chan<- struct{}) func() {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return func() {}
	}
	if err := w.Add(path); err != nil {
		w.Close()
		return func() {}
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-w.Events:
				if !ok {
					return
				}
				select {
				case wake <- struct{}{}:
				default:
				}
			case _, ok := <-w.Errors:
				if !ok {
					return
				}
			}
		}
	}()
	return func() { w.Close() }
}

// resolveTime returns the timestamp for a message. When useHeader is set and
// the payload looks like a CDR-encapsulated message whose first field is a
// std_msgs/Header, the embedded stamp (sec int32, nanosec uint32) is used;
// otherwise — or when the stamp is unset — the MCAP log time is used.
//
// ROS2 CDR layout: byte 0 is unused, byte 1 selects byte order (0 = big, 1 =
// little), bytes 2-3 are options, then the 4-byte-aligned payload begins. A
// leading Header places sec at [4:8] and nanosec at [8:12].
func resolveTime(useHeader bool, m *mcap.Message) time.Time {
	if useHeader && len(m.Data) >= 12 {
		var (
			sec  int32
			nsec uint32
		)
		switch m.Data[1] {
		case 0:
			sec = int32(binary.BigEndian.Uint32(m.Data[4:8]))
			nsec = binary.BigEndian.Uint32(m.Data[8:12])
		case 1:
			sec = int32(binary.LittleEndian.Uint32(m.Data[4:8]))
			nsec = binary.LittleEndian.Uint32(m.Data[8:12])
		}
		if sec != 0 || nsec != 0 {
			return time.Unix(int64(sec), int64(nsec))
		}
	}
	return time.Unix(0, int64(m.LogTime))
}
