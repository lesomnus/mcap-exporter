# mcap-exporter

Tail one or more [MCAP](https://mcap.dev) files that are being written by
`ros2 bag record` and export each ROS2 topic's message rate as an OpenTelemetry
metric, stamped with the **recorded message time** rather than wall-clock time.

## How it works

- **Tail**: a live bag has no footer/summary yet, so the indexed reader is
  useless. mcap-exporter streams each file forward with the MCAP lexer over a
  reader that blocks at EOF until more bytes are appended (and stops when the
  file is finalized or on shutdown). Directories are watched for new split
  files; each file is tailed by its own goroutine.
- **Bucket**: messages are counted into fixed message-time buckets (`bucket`,
  default `1s`) per topic. A bucket is emitted once the topic's latest message
  time has passed the bucket end plus `grace`.
- **Export**: each sealed bucket is pushed (with `topic`/`schema` attributes and
  the bucket's `[start, end)` message-time window) directly through the
  configured exporter's `Export`, bypassing the SDK's collection-time stamping
  so the timestamps are the recording's own.

The metrics carry no file name. Per `(topic, bucket)`:

| Metric | Type | Reveals |
|---|---|---|
| `mcap.topic.message.count` | delta `Sum` | rate — `Hz = count / bucket` |
| `mcap.topic.message.bytes` | delta `Sum` (`By`) | bandwidth — `B/s = bytes / bucket` |
| `mcap.topic.interval.max` | `Gauge` (`ns`) | largest gap between consecutive messages — catches a stall a Hz average hides |

## Quick start

```sh
# Eyeball metrics from a finished bag on stdout.
$ mcap-exporter --config mcap-exporter.yaml watch ./my_bag.mcap
```

```yaml
# mcap-exporter.yaml
mcap:
  paths: [/data/bag]   # files or directories; also accepted as `watch` arguments
  exporter: otlp/local # an otel.exporters id (otlp or debug; prometheus is pull-only)
  bucket: 1s
  flush_interval: 1s
  state_path: /var/lib/mcap-exporter/state.json
otel:
  exporters:
    otlp/local: { endpoint: 127.0.0.1:4317, tls: { insecure: true } }
    debug: {}        # writes exported metrics to stdout
```

> Do **not** list the metric exporter under `otel.providers`. mcap-exporter
> extracts the exporter directly; wiring it into a provider would add a periodic
> reader that re-stamps points with collection time.

## Recording for low latency

Near-real-time is a property of how the bag is **recorded**, not of the reader:
a chunked, compressed bag (the `ros2 bag` default, ~768 KiB chunks) hides every
message until the whole chunk is flushed. For per-message latency, record
unchunked (or with a small chunk size):

```yaml
# storage_opts.yaml
noChunking: true        # or: chunkSize: 65536
```

```sh
$ ros2 bag record -s mcap --storage-config-file storage_opts.yaml --all
```

## Computing Hz in ClickHouse

Delta points carry their own window, so the rate is a per-row division and
restarts need no special handling:

```sql
SELECT
    Attributes['topic'] AS topic,
    TimeUnix,
    max(Value) AS count,                 -- collapse deterministic re-sends
    max(Value) / nullif(
        (toUnixTimestamp64Nano(TimeUnix) - toUnixTimestamp64Nano(StartTimeUnix)) / 1e9, 0) AS hz
FROM otel_metrics_sum
WHERE MetricName = 'mcap.topic.message.count' AND AggregationTemporality = 1
GROUP BY topic, TimeUnix, StartTimeUnix
ORDER BY topic, TimeUnix;
```

`mcap.topic.message.bytes` is the same `otel_metrics_sum` query (`bytes / window`
= bandwidth). `mcap.topic.interval.max` is a gauge in `otel_metrics_gauge`; read
its `Value` directly (e.g. alert on `max(Value) > 5e8` for a >0.5 s stall).

## Recent window

`recent: <duration>` exports only buckets within that window of the recording's
latest message time. Files are still read from the start (a live MCAP has no
time index to seek by, and topic/channel definitions live at the front), so this
bounds **export volume**, not disk reads: the floor is locked to
`(latest message time − recent)` once the backlog has been read. Unset exports
the whole timeline; `recent: 0s` exports only from the current tail onward.

## Restarts and duplicates

Points are deterministic — the same `(topic, bucket)` always yields the same
value — so a re-read produces byte-identical rows that collapse with the
`GROUP BY`/`max(Value)` above. `state_path` additionally checkpoints the last
emitted bucket per topic so a normal restart re-emits nothing.

## Generating a test fixture

```sh
$ go run ./internal/mcaptest/gen sample.mcap   # /scan @ 10 Hz, /tf @ 2 Hz
```
