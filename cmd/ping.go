package cmd

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	redisclient "github.com/adtyap26/redmove/internal/redis"
	"github.com/spf13/cobra"
)

func newPingCmd() *cobra.Command {
	var count int

	pingCmd := &cobra.Command{
		Use:   "ping",
		Short: "Verify Redis connectivity and measure latency",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPing(cmd.Context(), count)
		},
	}

	pingCmd.Flags().IntVarP(&count, "count", "n", 10, "Number of pings to send")
	return pingCmd
}

func runPing(ctx context.Context, count int) error {
	client, err := redisclient.NewClient(OptsFromGlobals())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	uri := Globals.URI
	if uri == "" {
		uri = "redis://localhost:6379"
	}
	fmt.Printf("PING %s — %d pings\n\n", uri, count)

	var durations []time.Duration
	var errors int

	for i := range count {
		start := time.Now()
		_, err := client.Ping(ctx).Result()
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("  [%d] error: %v\n", i+1, err)
			errors++
			continue
		}
		durations = append(durations, elapsed)
	}

	fmt.Println()

	if len(durations) == 0 {
		fmt.Printf("  ok: 0/%d    errors: %d/%d\n", count, errors, count)
		return fmt.Errorf("all %d pings failed", count)
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	var total time.Duration
	for _, d := range durations {
		total += d
	}
	avg := total / time.Duration(len(durations))

	fmt.Printf("  min    %s\n", fmtDuration(durations[0]))
	fmt.Printf("  avg    %s\n", fmtDuration(avg))
	fmt.Printf("  p50    %s\n", fmtDuration(percentile(durations, 0.50)))
	fmt.Printf("  p95    %s\n", fmtDuration(percentile(durations, 0.95)))
	fmt.Printf("  p99    %s\n", fmtDuration(percentile(durations, 0.99)))
	fmt.Printf("  max    %s\n", fmtDuration(durations[len(durations)-1]))
	fmt.Println()
	fmt.Printf("  ok: %d/%d    errors: %d/%d\n", len(durations), count, errors, count)

	return nil
}

// percentile returns the value at the given percentile from a sorted slice.
func percentile(sorted []time.Duration, pct float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(pct*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// fmtDuration formats a duration as milliseconds with 2 decimal places.
func fmtDuration(d time.Duration) string {
	ms := float64(d) / float64(time.Millisecond)
	return fmt.Sprintf("%.2fms", ms)
}
