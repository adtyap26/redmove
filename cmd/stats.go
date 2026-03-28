package cmd

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/adtyap26/redmove/internal/pipeline"
	redisclient "github.com/adtyap26/redmove/internal/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

func newStatsCmd() *cobra.Command {
	var (
		match      string
		topN       int
		keyTypes   []string
		keyInclude []string
		keyExclude []string
	)

	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Analyze keyspace: types, prefixes, TTL distribution",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStats(cmd.Context(), statsConfig{
				match:      match,
				topN:       topN,
				keyTypes:   keyTypes,
				keyInclude: keyInclude,
				keyExclude: keyExclude,
			})
		},
	}

	f := cmd.Flags()
	f.StringVar(&match, "match", "*", "Key pattern to analyze")
	f.IntVar(&topN, "top", 20, "Number of top prefixes to show")
	f.StringSliceVar(&keyTypes, "key-type", nil, "Filter by Redis type (e.g. hash,string)")
	f.StringSliceVar(&keyInclude, "key-include", nil, "Include keys matching glob pattern (repeatable)")
	f.StringSliceVar(&keyExclude, "key-exclude", nil, "Exclude keys matching glob pattern (repeatable)")

	return cmd
}

type keyspaceStats struct {
	totalKeys  int64
	typeCounts map[string]int64
	prefixes   map[string]int64
	ttlBuckets [6]int64 // no-expiry, <1m, 1m-1h, 1h-24h, 1d-7d, >7d
}

type statsConfig struct {
	match      string
	topN       int
	keyTypes   []string
	keyInclude []string
	keyExclude []string
}

func runStats(ctx context.Context, cfg statsConfig) error {
	client, err := redisclient.NewClient(OptsFromGlobals())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	uri := Globals.URI
	if uri == "" {
		uri = "redis://localhost:6379"
	}
	fmt.Printf("Analyzing keyspace: %s (match: %q)\n\n", uri, cfg.match)

	start := time.Now()
	ks := &keyspaceStats{
		typeCounts: make(map[string]int64),
		prefixes:   make(map[string]int64),
	}

	// Use scanKeys via a collecting callback.
	ch := make(chan pipeline.Record, Globals.QueueSize)
	scanOpts := redisclient.ScanOpts{
		Match:     cfg.match,
		Count:     1000,
		BatchSize: Globals.BatchSize,
		KeyTypes:  cfg.keyTypes,
		Include:   cfg.keyInclude,
		Exclude:   cfg.keyExclude,
	}

	// Scan in a goroutine, aggregate in the main goroutine.
	scanErr := make(chan error, 1)
	go func() {
		scanErr <- scanAndEnrich(ctx, client, scanOpts, ch)
	}()

	for rec := range ch {
		ks.totalKeys++
		ks.typeCounts[rec.Type]++
		ks.prefixes[extractPrefix(rec.Key)]++
		bucketTTL(ks, rec.TTL)
	}

	if err := <-scanErr; err != nil {
		return fmt.Errorf("scan failed: %w", err)
	}

	elapsed := time.Since(start)

	// Print results.
	fmt.Printf("Total keys: %d (scanned in %s)\n\n", ks.totalKeys, elapsed.Truncate(time.Millisecond))

	// Type distribution.
	fmt.Println("Type Distribution:")
	typeOrder := sortedKeys(ks.typeCounts)
	for _, t := range typeOrder {
		pct := float64(ks.typeCounts[t]) / float64(ks.totalKeys) * 100
		fmt.Printf("  %-10s %8d  (%5.1f%%)\n", t, ks.typeCounts[t], pct)
	}
	fmt.Println()

	// Top prefixes.
	fmt.Printf("Top %d Prefixes:\n", cfg.topN)
	type prefixCount struct {
		prefix string
		count  int64
	}
	var pcs []prefixCount
	for p, c := range ks.prefixes {
		pcs = append(pcs, prefixCount{p, c})
	}
	sort.Slice(pcs, func(i, j int) bool { return pcs[i].count > pcs[j].count })
	if len(pcs) > cfg.topN {
		pcs = pcs[:cfg.topN]
	}
	for _, pc := range pcs {
		pct := float64(pc.count) / float64(ks.totalKeys) * 100
		fmt.Printf("  %-30s %8d  (%5.1f%%)\n", pc.prefix, pc.count, pct)
	}
	fmt.Println()

	// TTL distribution.
	ttlLabels := [6]string{"no expiry", "< 1 min", "1m – 1h", "1h – 24h", "1d – 7d", "> 7 days"}
	fmt.Println("TTL Distribution:")
	for i, label := range ttlLabels {
		if ks.ttlBuckets[i] == 0 {
			continue
		}
		pct := float64(ks.ttlBuckets[i]) / float64(ks.totalKeys) * 100
		fmt.Printf("  %-12s %8d  (%5.1f%%)\n", label, ks.ttlBuckets[i], pct)
	}

	return nil
}

// scanAndEnrich scans keys with TYPE + PTTL and sends to ch, then closes it.
func scanAndEnrich(ctx context.Context, client goredis.UniversalClient, opts redisclient.ScanOpts, ch chan<- pipeline.Record) error {
	reader := redisclient.NewScanReader(client, opts)
	return reader.Read(ctx, ch)
}

func extractPrefix(key string) string {
	if idx := strings.Index(key, ":"); idx != -1 {
		return key[:idx+1]
	}
	return key
}

func bucketTTL(ks *keyspaceStats, ttl time.Duration) {
	switch {
	case ttl < 0: // -1 = no expiry, -2 = key doesn't exist
		ks.ttlBuckets[0]++
	case ttl < time.Minute:
		ks.ttlBuckets[1]++
	case ttl < time.Hour:
		ks.ttlBuckets[2]++
	case ttl < 24*time.Hour:
		ks.ttlBuckets[3]++
	case ttl < 7*24*time.Hour:
		ks.ttlBuckets[4]++
	default:
		ks.ttlBuckets[5]++
	}
}

func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return m[keys[i]] > m[keys[j]] })
	return keys
}
