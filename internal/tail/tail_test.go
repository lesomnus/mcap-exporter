package tail

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/lesomnus/mcap-exporter/internal/mcaptest"
	"github.com/stretchr/testify/require"
)

// drainStatic tails a finalized file to completion and returns the messages.
func drainStatic(t *testing.T, path string, headers map[string]bool) []Msg {
	t.Helper()
	out := make(chan Msg, 256)
	var got []Msg
	done := make(chan struct{})
	go func() {
		for m := range out {
			got = append(got, m)
		}
		close(done)
	}()

	err := TailOne(context.Background(), path, nil, headers, 10*time.Millisecond, out)
	require.NoError(t, err)
	close(out)
	<-done
	return got
}

func TestTailOne_Static(t *testing.T) {
	for _, tc := range []struct {
		name      string
		chunkSize int64
	}{
		{"unchunked", 0},
		{"chunked_zstd", 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "a.mcap")
			require.NoError(t, mcaptest.Write(path, tc.chunkSize, []mcaptest.Spec{
				{Topic: "/chatter", Schema: "std_msgs/msg/String", LogTime: 1_000_000_000},
				{Topic: "/chatter", Schema: "std_msgs/msg/String", LogTime: 1_500_000_000},
				{Topic: "/imu", Schema: "sensor_msgs/msg/Imu", LogTime: 2_000_000_000},
			}))

			got := drainStatic(t, path, nil)
			require.Len(t, got, 3)

			sort.Slice(got, func(i, j int) bool { return got[i].Time.Before(got[j].Time) })
			require.Equal(t, "/chatter", got[0].Topic)
			require.Equal(t, "std_msgs/msg/String", got[0].Schema)
			require.Equal(t, time.Unix(0, 1_000_000_000), got[0].Time)
			require.Equal(t, "/imu", got[2].Topic)
			require.Equal(t, "sensor_msgs/msg/Imu", got[2].Schema)
		})
	}
}

func TestTailOne_HeaderStampOptIn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.mcap")
	require.NoError(t, mcaptest.Write(path, 0, []mcaptest.Spec{
		{Topic: "/imu", Schema: "sensor_msgs/msg/Imu", LogTime: 9_999, Data: mcaptest.HeaderPayload(100, 500)},
	}))

	// Without opt-in, the MCAP log time wins.
	got := drainStatic(t, path, nil)
	require.Len(t, got, 1)
	require.Equal(t, time.Unix(0, 9_999), got[0].Time)

	// With opt-in for /imu, the embedded Header stamp wins.
	got = drainStatic(t, path, map[string]bool{"/imu": true})
	require.Len(t, got, 1)
	require.Equal(t, time.Unix(100, 500), got[0].Time)
}

func TestResolveTime(t *testing.T) {
	t.Run("log_time_default", func(t *testing.T) {
		m := &mcap.Message{LogTime: 42, Data: mcaptest.HeaderPayload(100, 500)}
		require.Equal(t, time.Unix(0, 42), resolveTime(false, m))
	})
	t.Run("header_when_opted_in", func(t *testing.T) {
		m := &mcap.Message{LogTime: 42, Data: mcaptest.HeaderPayload(100, 500)}
		require.Equal(t, time.Unix(100, 500), resolveTime(true, m))
	})
	t.Run("falls_back_when_stamp_zero", func(t *testing.T) {
		m := &mcap.Message{LogTime: 42, Data: mcaptest.HeaderPayload(0, 0)}
		require.Equal(t, time.Unix(0, 42), resolveTime(true, m))
	})
	t.Run("falls_back_when_payload_short", func(t *testing.T) {
		m := &mcap.Message{LogTime: 42, Data: []byte{0, 1}}
		require.Equal(t, time.Unix(0, 42), resolveTime(true, m))
	})
}

func TestTailOne_FiresIdleOnCatchUp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.mcap")
	require.NoError(t, mcaptest.Write(path, 0, []mcaptest.Spec{
		{Topic: "/t", LogTime: 1_000_000_000},
	}))

	out := make(chan Msg, 4)
	go func() {
		for range out { // drain
		}
	}()
	idled := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = TailOne(ctx, path, func() { close(idled) }, nil, 10*time.Millisecond, out)
	}()

	select {
	case <-idled:
	case <-time.After(3 * time.Second):
		t.Fatal("idle was not fired after catching up")
	}
}

func TestTailOne_LiveAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.mcap")
	f, err := os.Create(path)
	require.NoError(t, err)
	b, err := mcaptest.NewBuilder(f, 0) // unchunked: each message flushes to disk
	require.NoError(t, err)

	// Two messages exist before tailing starts.
	require.NoError(t, b.Message("/t", "S", 1_000_000_000, nil))
	require.NoError(t, b.Message("/t", "S", 1_100_000_000, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan Msg, 16)
	go func() { _ = TailOne(ctx, path, nil, nil, 20*time.Millisecond, out) }()

	recv := func() Msg {
		t.Helper()
		select {
		case m := <-out:
			return m
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for a message")
			return Msg{}
		}
	}

	m0, m1 := recv(), recv()

	// Append more while the tailer is running; it must pick them up.
	require.NoError(t, b.Message("/t", "S", 1_200_000_000, nil))
	require.NoError(t, b.Message("/t", "S", 1_300_000_000, nil))
	m2, m3 := recv(), recv()

	cancel()
	require.NoError(t, f.Close())

	times := []time.Time{m0.Time, m1.Time, m2.Time, m3.Time}
	require.Equal(t, []time.Time{
		time.Unix(0, 1_000_000_000),
		time.Unix(0, 1_100_000_000),
		time.Unix(0, 1_200_000_000),
		time.Unix(0, 1_300_000_000),
	}, times)
}
