package config

import (
	"os"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/lesomnus/z"
)

var DefaultConfigPaths = []string{
	"mcap-exporter.yaml",
	"mcap-exporter.yml",
}

type Config struct {
	path string

	Mcap McapConfig

	Otel OtelConfig
}

func ReadFromFile(p string) (*Config, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, z.Err(err, "open")
	}

	var c Config
	if err := yaml.NewDecoder(f).Decode(&c); err != nil {
		return nil, z.Err(err, "decode")
	}

	c.path = p
	return &c, nil
}

func (c *Config) Path() string {
	return c.path
}

func (c *Config) Evaluate() error {
	z.FallbackP(&c.Mcap.Bucket, Duration(time.Second))
	z.FallbackP(&c.Mcap.Grace, Duration(time.Second))
	z.FallbackP(&c.Mcap.FlushInterval, Duration(time.Second))
	z.FallbackP(&c.Mcap.PollInterval, Duration(250*time.Millisecond))
	return nil
}
