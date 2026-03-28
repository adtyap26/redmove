package cmd

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"time"

	"github.com/adtyap26/redmove/internal/pipeline"
	redisclient "github.com/adtyap26/redmove/internal/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

func newCompareCmd() *cobra.Command {
	var (
		sourceURI      string
		targetURI      string
		sample         int
		match          string
		targetCluster  bool
		targetPassword string
		targetUsername string
		targetDB       int
	)

	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Compare keys between source and target Redis (sample-based diff)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCompare(cmd.Context(), compareConfig{
				sourceURI:      sourceURI,
				targetURI:      targetURI,
				sample:         sample,
				match:          match,
				targetCluster:  targetCluster,
				targetPassword: targetPassword,
				targetUsername: targetUsername,
				targetDB:       targetDB,
			})
		},
	}

	f := cmd.Flags()
	f.StringVar(&sourceURI, "source", "", "Source Redis URI (required)")
	f.StringVar(&targetURI, "target", "", "Target Redis URI (required)")
	f.IntVar(&sample, "sample", 10000, "Number of keys to sample")
	f.StringVar(&match, "match", "*", "Key pattern to compare")
	f.BoolVar(&targetCluster, "target-cluster", false, "Target is a cluster")
	f.StringVar(&targetPassword, "target-password", "", "Target Redis password")
	f.StringVar(&targetUsername, "target-username", "", "Target Redis ACL username")
	f.IntVar(&targetDB, "target-db", 0, "Target Redis database number")

	cmd.MarkFlagRequired("source")
	cmd.MarkFlagRequired("target")

	return cmd
}

type compareConfig struct {
	sourceURI      string
	targetURI      string
	sample         int
	match          string
	targetCluster  bool
	targetPassword string
	targetUsername string
	targetDB       int
}

type compareResult struct {
	sampled      int
	missing      int
	typeMismatch int
	valueDiff    int
	ttlDiff      int
	matching     int
}

func runCompare(ctx context.Context, cfg compareConfig) error {
	srcOpts := OptsFromGlobals()
	srcOpts.URI = cfg.sourceURI
	srcClient, err := redisclient.NewClient(srcOpts)
	if err != nil {
		return fmt.Errorf("source connect: %w", err)
	}
	defer srcClient.Close()

	dstClient, err := redisclient.NewClient(redisclient.ConnectOpts{
		URI:      cfg.targetURI,
		Cluster:  cfg.targetCluster,
		Password: cfg.targetPassword,
		Username: cfg.targetUsername,
		DB:       cfg.targetDB,
	})
	if err != nil {
		return fmt.Errorf("target connect: %w", err)
	}
	defer dstClient.Close()

	fmt.Printf("Comparing %s → %s (sample: %d, match: %q)\n\n", cfg.sourceURI, cfg.targetURI, cfg.sample, cfg.match)

	start := time.Now()

	// Phase 1: Reservoir sampling — collect random sample of keys from source.
	keys, err := reservoirSample(ctx, srcClient, cfg.match, cfg.sample)
	if err != nil {
		return fmt.Errorf("sample keys: %w", err)
	}

	if len(keys) == 0 {
		fmt.Println("No keys found matching pattern.")
		return nil
	}

	// Phase 2: Compare each sampled key.
	result := compareResult{sampled: len(keys)}
	batchSize := Globals.BatchSize
	if batchSize <= 0 {
		batchSize = 50
	}

	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]

		if err := compareBatch(ctx, srcClient, dstClient, batch, &result); err != nil {
			return fmt.Errorf("compare batch: %w", err)
		}
	}

	elapsed := time.Since(start)

	// Print results.
	pct := func(n int) float64 {
		if result.sampled == 0 {
			return 0
		}
		return float64(n) / float64(result.sampled) * 100
	}

	fmt.Printf("  Sampled:        %d keys\n", result.sampled)
	fmt.Printf("  Matching:       %5d  (%5.1f%%)\n", result.matching, pct(result.matching))
	fmt.Printf("  Missing:        %5d  (%5.1f%%)\n", result.missing, pct(result.missing))
	fmt.Printf("  Type mismatch:  %5d  (%5.1f%%)\n", result.typeMismatch, pct(result.typeMismatch))
	fmt.Printf("  Value diff:     %5d  (%5.1f%%)\n", result.valueDiff, pct(result.valueDiff))
	fmt.Printf("  TTL diff:       %5d  (%5.1f%%)\n", result.ttlDiff, pct(result.ttlDiff))
	fmt.Printf("\n  Elapsed: %s\n", elapsed.Truncate(time.Millisecond))

	return nil
}

// reservoirSample uses Algorithm R to collect a uniform random sample of keys.
func reservoirSample(ctx context.Context, client goredis.UniversalClient, match string, sampleSize int) ([]string, error) {
	ch := make(chan pipeline.Record, 10000)
	scanOpts := redisclient.ScanOpts{Match: match, Count: 1000, BatchSize: 100}

	scanErr := make(chan error, 1)
	go func() {
		reader := redisclient.NewScanReader(client, scanOpts)
		scanErr <- reader.Read(ctx, ch)
	}()

	reservoir := make([]string, 0, sampleSize)
	i := 0

	for rec := range ch {
		if i < sampleSize {
			reservoir = append(reservoir, rec.Key)
		} else {
			j := rand.IntN(i + 1)
			if j < sampleSize {
				reservoir[j] = rec.Key
			}
		}
		i++
	}

	if err := <-scanErr; err != nil {
		return nil, err
	}

	return reservoir, nil
}

// compareBatch compares a batch of keys between source and target.
func compareBatch(ctx context.Context, src, dst goredis.UniversalClient, keys []string, result *compareResult) error {
	// Pipeline EXISTS + TYPE + PTTL on target for all keys.
	dstPipe := dst.Pipeline()
	existsCmds := make([]*goredis.IntCmd, len(keys))
	dstTypeCmds := make([]*goredis.StatusCmd, len(keys))
	dstTTLCmds := make([]*goredis.DurationCmd, len(keys))

	for i, key := range keys {
		existsCmds[i] = dstPipe.Exists(ctx, key)
		dstTypeCmds[i] = dstPipe.Type(ctx, key)
		dstTTLCmds[i] = dstPipe.PTTL(ctx, key)
	}
	if _, err := dstPipe.Exec(ctx); err != nil && err != goredis.Nil {
		return err
	}

	// Pipeline TYPE + PTTL on source for all keys.
	srcPipe := src.Pipeline()
	srcTypeCmds := make([]*goredis.StatusCmd, len(keys))
	srcTTLCmds := make([]*goredis.DurationCmd, len(keys))

	for i, key := range keys {
		srcTypeCmds[i] = srcPipe.Type(ctx, key)
		srcTTLCmds[i] = srcPipe.PTTL(ctx, key)
	}
	if _, err := srcPipe.Exec(ctx); err != nil && err != goredis.Nil {
		return err
	}

	for i, key := range keys {
		// Check existence.
		if existsCmds[i].Val() == 0 {
			result.missing++
			continue
		}

		srcType := srcTypeCmds[i].Val()
		dstType := dstTypeCmds[i].Val()

		// Compare type.
		if srcType != dstType {
			result.typeMismatch++
			continue
		}

		// Compare values.
		srcVal, err := redisclient.ReadValue(ctx, src, key, srcType)
		if err != nil {
			continue
		}
		dstVal, err := redisclient.ReadValue(ctx, dst, key, dstType)
		if err != nil {
			continue
		}

		if !reflect.DeepEqual(srcVal, dstVal) {
			result.valueDiff++
			continue
		}

		// Compare TTL (5s tolerance).
		srcTTL := srcTTLCmds[i].Val()
		dstTTL := dstTTLCmds[i].Val()
		diff := srcTTL - dstTTL
		if diff < 0 {
			diff = -diff
		}
		if diff > 5*time.Second {
			result.ttlDiff++
			continue
		}

		result.matching++
	}
	return nil
}
