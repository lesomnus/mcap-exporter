// Command gen writes a sample MCAP file for manual testing of the watch
// command: /scan at ~10 Hz and /tf at ~2 Hz over a few seconds of log time.
package main

import (
	"fmt"
	"os"

	"github.com/lesomnus/mcap-exporter/internal/mcaptest"
)

func main() {
	out := "sample.mcap"
	if len(os.Args) > 1 {
		out = os.Args[1]
	}

	const base = 1_700_000_000 * 1_000_000_000 // a fixed, realistic epoch (ns)
	var msgs []mcaptest.Spec
	// /scan: 10 Hz for 4 seconds.
	for i := 0; i < 40; i++ {
		msgs = append(msgs, mcaptest.Spec{
			Topic:   "/scan",
			Schema:  "sensor_msgs/msg/LaserScan",
			LogTime: uint64(base + int64(i)*100_000_000),
		})
	}
	// /tf: 2 Hz for 4 seconds.
	for i := 0; i < 8; i++ {
		msgs = append(msgs, mcaptest.Spec{
			Topic:   "/tf",
			Schema:  "tf2_msgs/msg/TFMessage",
			LogTime: uint64(base + int64(i)*500_000_000),
		})
	}

	if err := mcaptest.Write(out, 0, msgs); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Println("wrote", out)
}
