// Package mcaptest builds small MCAP files for tests, mirroring how a ROS2 bag
// lays out schemas, channels and messages.
package mcaptest

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/foxglove/mcap/go/mcap"
)

// Builder incrementally writes an MCAP stream, assigning schema/channel ids per
// topic on first use. It supports both one-shot fixtures and live-append tests
// (write some messages, leave the file open, write more).
type Builder struct {
	w        *mcap.Writer
	schemaID map[string]uint16
	chanID   map[string]uint16
	next     uint16
}

// NewBuilder writes the MCAP magic+header to w. chunkSize > 0 produces a zstd
// chunked file; chunkSize == 0 writes records directly (unchunked, per-message
// readable) which is what live tailing needs.
func NewBuilder(w io.Writer, chunkSize int64) (*Builder, error) {
	opts := &mcap.WriterOptions{}
	if chunkSize > 0 {
		opts.Chunked = true
		opts.ChunkSize = chunkSize
		opts.Compression = mcap.CompressionZSTD
	}
	mw, err := mcap.NewWriter(w, opts)
	if err != nil {
		return nil, err
	}
	if err := mw.WriteHeader(&mcap.Header{Profile: "ros2", Library: "mcaptest"}); err != nil {
		return nil, err
	}
	return &Builder{
		w:        mw,
		schemaID: map[string]uint16{},
		chanID:   map[string]uint16{},
		next:     1,
	}, nil
}

// Message writes one message on (topic, schema), declaring the schema and
// channel on first use.
func (b *Builder) Message(topic, schema string, logTime uint64, data []byte) error {
	cid, ok := b.chanID[topic]
	if !ok {
		sid := b.schemaID[schema]
		if sid == 0 {
			sid = b.next
			b.next++
			b.schemaID[schema] = sid
			if err := b.w.WriteSchema(&mcap.Schema{ID: sid, Name: schema, Encoding: "ros2msg"}); err != nil {
				return err
			}
		}
		cid = b.next
		b.next++
		b.chanID[topic] = cid
		if err := b.w.WriteChannel(&mcap.Channel{ID: cid, SchemaID: sid, Topic: topic, MessageEncoding: "cdr"}); err != nil {
			return err
		}
	}
	return b.w.WriteMessage(&mcap.Message{
		ChannelID:   cid,
		LogTime:     logTime,
		PublishTime: logTime,
		Data:        data,
	})
}

// Close finalizes the file (writes the summary + footer).
func (b *Builder) Close() error { return b.w.Close() }

// Spec describes one message for the Write convenience helper.
type Spec struct {
	Topic   string
	Schema  string
	LogTime uint64
	Data    []byte
}

// Write creates a finalized MCAP file at path containing msgs.
func Write(path string, chunkSize int64, msgs []Spec) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := NewBuilder(f, chunkSize)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		schema := m.Schema
		if schema == "" {
			schema = "std_msgs/msg/Empty"
		}
		if err := b.Message(m.Topic, schema, m.LogTime, m.Data); err != nil {
			return err
		}
	}
	return b.Close()
}

// HeaderPayload builds a CDR little-endian message payload whose first field is
// a std_msgs/Header with the given stamp (sec, nanosec).
func HeaderPayload(sec int32, nsec uint32) []byte {
	p := make([]byte, 12)
	p[0] = 0x00 // CDR encapsulation: unused
	p[1] = 0x01 // byte order: little-endian
	binary.LittleEndian.PutUint32(p[4:8], uint32(sec))
	binary.LittleEndian.PutUint32(p[8:12], nsec)
	return p
}
