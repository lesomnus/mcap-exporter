package config

import (
	"encoding"
	"time"
)

// Duration is a [time.Duration] that (de)serializes from a human string such as
// "1s" or "250ms". goccy/go-yaml honors [encoding.TextMarshaler]/[encoding.TextUnmarshaler],
// so a bare time.Duration would not round-trip; this named type does.
type Duration time.Duration

var (
	_ encoding.TextMarshaler   = Duration(0)
	_ encoding.TextUnmarshaler = (*Duration)(nil)
)

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}
