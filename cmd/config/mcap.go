package config

// McapConfig configures the `watch` command: which MCAP files to tail and how
// the per-topic message-rate metric is bucketed and exported.
type McapConfig struct {
	// Paths is a list of MCAP files or directories to watch. A directory is
	// scanned for *.mcap files and watched for new split files.
	Paths []string `yaml:"paths,omitempty"`

	// Exporter is the id of a metric exporter declared under `otel.exporters`
	// to push to (e.g. "otlp/local" or "debug"). It must support metric export
	// (otlp or debug); pull-only exporters such as prometheus are rejected.
	Exporter string `yaml:"exporter,omitempty"`

	// Bucket is the message-time bucket width. One delta data point is emitted
	// per (topic, bucket); Hz = count / bucket. Default 1s.
	Bucket Duration `yaml:"bucket,omitempty"`

	// Grace is extra message-time slack before a bucket is sealed, to tolerate
	// slightly out-of-order messages. Default 1s.
	Grace Duration `yaml:"grace,omitempty"`

	// FlushInterval is the wall-clock cadence at which sealed buckets are
	// exported. Default 1s.
	FlushInterval Duration `yaml:"flush_interval,omitempty"`

	// PollInterval is the fallback poll period for the tail reader when no
	// filesystem write event arrives. Default 250ms.
	PollInterval Duration `yaml:"poll_interval,omitempty"`

	// Recent, when set, exports only buckets within this window of the
	// recording's latest message time: the files are read from the start (to
	// resolve topics) but the export floor is locked to (latest − recent) once
	// the backlog has been read. Unset exports the entire timeline; "0s" exports
	// only from the current tail onward. This bounds export volume, not disk I/O
	// (a live MCAP has no time index to seek by).
	Recent *Duration `yaml:"recent,omitempty"`

	// StatePath is a file where the last-emitted bucket per topic is
	// checkpointed so a restart does not re-emit already-exported buckets.
	// Empty disables checkpointing (re-sends are still deterministic and
	// dedupable downstream).
	StatePath string `yaml:"state_path,omitempty"`

	// Headers lists topics whose ROS2 message begins with a std_msgs/Header;
	// for these the message stamp is read from Header.stamp instead of the MCAP
	// log time. Topics not listed always use the MCAP log time.
	Headers []string `yaml:"headers,omitempty"`
}

// HeaderSet returns the Headers list as a lookup set.
func (c McapConfig) HeaderSet() map[string]bool {
	if len(c.Headers) == 0 {
		return nil
	}
	out := make(map[string]bool, len(c.Headers))
	for _, t := range c.Headers {
		out[t] = true
	}
	return out
}
