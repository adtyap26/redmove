package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/adtyap26/redmove/internal/pipeline"
	redisclient "github.com/adtyap26/redmove/internal/redis"
	"github.com/spf13/cobra"
)

func newReplicateCmd() *cobra.Command {
	var (
		sourceURI      string
		targetURI      string
		match          string
		mode           string
		useStruct      bool
		targetCluster  bool
		targetPassword string
		targetUsername string
		targetDB       int
		scanCount      int
		keyTypes       []string
		keyInclude     []string
		keyExclude     []string
		memLimit       string
		streamIDMode   string
		migrateIndexes bool
	)

	cmd := &cobra.Command{
		Use:   "replicate",
		Short: "Replicate keys from source to target Redis",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReplicate(cmd.Context(), replicateConfig{
				sourceURI:      sourceURI,
				targetURI:      targetURI,
				match:          match,
				mode:           mode,
				useStruct:      useStruct,
				targetCluster:  targetCluster,
				targetPassword: targetPassword,
				targetUsername: targetUsername,
				targetDB:       targetDB,
				scanCount:      scanCount,
				keyTypes:       keyTypes,
				keyInclude:     keyInclude,
				keyExclude:     keyExclude,
				memLimit:       memLimit,
				streamIDMode:   streamIDMode,
				migrateIndexes: migrateIndexes,
			})
		},
	}

	f := cmd.Flags()
	f.StringVar(&sourceURI, "source", "", "Source Redis URI (required)")
	f.StringVar(&targetURI, "target", "", "Target Redis URI (required)")
	f.StringVar(&match, "match", "*", "Key pattern to replicate")
	f.StringVar(&mode, "mode", "scan", "Replication mode: scan, live, or liveonly")
	f.BoolVar(&useStruct, "struct", false, "Use struct mode (type-specific commands) instead of DUMP/RESTORE")
	f.BoolVar(&targetCluster, "target-cluster", false, "Target is a cluster")
	f.StringVar(&targetPassword, "target-password", "", "Target Redis password")
	f.StringVar(&targetUsername, "target-username", "", "Target Redis ACL username")
	f.IntVar(&targetDB, "target-db", 0, "Target Redis database number")
	f.IntVar(&scanCount, "scan-count", 5000, "SCAN COUNT hint (keys per SCAN call)")
	f.StringSliceVar(&keyTypes, "key-type", nil, "Filter by Redis type (e.g. hash,string)")
	f.StringSliceVar(&keyInclude, "key-include", nil, "Include keys matching glob pattern (repeatable)")
	f.StringSliceVar(&keyExclude, "key-exclude", nil, "Exclude keys matching glob pattern (repeatable)")
	f.StringVar(&memLimit, "mem-limit", "", "Skip keys larger than this (e.g. 10MB, 1GB)")
	f.StringVar(&streamIDMode, "stream-id", "preserve", "Stream entry ID mode: preserve or reset")
	f.BoolVar(&migrateIndexes, "migrate-indexes", false, "Migrate RediSearch indexes after replication")

	cmd.MarkFlagRequired("source")
	cmd.MarkFlagRequired("target")

	return cmd
}

type replicateConfig struct {
	sourceURI      string
	targetURI      string
	match          string
	mode           string
	useStruct      bool
	targetCluster  bool
	targetPassword string
	targetUsername string
	targetDB       int
	scanCount      int
	keyTypes       []string
	keyInclude     []string
	keyExclude     []string
	memLimit       string
	streamIDMode   string
	migrateIndexes bool
}

func runReplicate(ctx context.Context, cfg replicateConfig) error {
	// Source connection inherits global flags.
	srcOpts := OptsFromGlobals()
	srcOpts.URI = cfg.sourceURI
	srcClient, err := redisclient.NewClient(srcOpts)
	if err != nil {
		return fmt.Errorf("source connect: %w", err)
	}
	defer srcClient.Close()

	// Target connection uses its own flags.
	dstOpts := redisclient.ConnectOpts{
		URI:      cfg.targetURI,
		Cluster:  cfg.targetCluster,
		Password: cfg.targetPassword,
		Username: cfg.targetUsername,
		DB:       cfg.targetDB,
	}
	dstClient, err := redisclient.NewClient(dstOpts)
	if err != nil {
		return fmt.Errorf("target connect: %w", err)
	}
	defer dstClient.Close()

	var memLimitBytes int64
	if cfg.memLimit != "" {
		var err error
		memLimitBytes, err = redisclient.ParseMemLimit(cfg.memLimit)
		if err != nil {
			return fmt.Errorf("invalid --mem-limit: %w", err)
		}
	}

	var reader pipeline.Reader
	var writer pipeline.Writer

	switch cfg.mode {
	case "live", "liveonly":
		reader = redisclient.NewNotificationReader(srcClient, redisclient.NotifyOpts{
			DB:        Globals.DB,
			Match:     cfg.match,
			UseStruct: cfg.useStruct,
			KeyTypes:  cfg.keyTypes,
			Include:   cfg.keyInclude,
			Exclude:   cfg.keyExclude,
		})
	case "scan", "":
		scanOpts := redisclient.ScanOpts{
			Match:     cfg.match,
			Count:     int64(cfg.scanCount),
			BatchSize: Globals.BatchSize,
			KeyTypes:  cfg.keyTypes,
			Include:   cfg.keyInclude,
			Exclude:   cfg.keyExclude,
			MemLimit:  memLimitBytes,
		}
		if cfg.useStruct {
			reader = redisclient.NewStructReader(srcClient, scanOpts)
		} else {
			reader = redisclient.NewDumpReader(srcClient, scanOpts)
		}
	default:
		return fmt.Errorf("unknown mode %q: use scan, live, or liveonly", cfg.mode)
	}

	if cfg.useStruct {
		writer = redisclient.NewStructWriter(dstClient, redisclient.StructWriteOpts{
			BatchSize:    Globals.BatchSize,
			StreamIDMode: cfg.streamIDMode,
		})
	} else {
		writer = redisclient.NewRestoreWriter(dstClient, redisclient.RestoreOpts{
			BatchSize: Globals.BatchSize,
			Replace:   true,
			Threads:   Globals.Threads,
		})
	}

	modeLabel := cfg.mode
	if modeLabel == "" {
		modeLabel = "scan"
	}
	transferMode := "dump/restore"
	if cfg.useStruct {
		transferMode = "struct"
	}

	if cfg.mode == "live" {
		fmt.Printf("Live replicating %s → %s (transfer: %s, match: %q)\n", cfg.sourceURI, cfg.targetURI, transferMode, cfg.match)
		fmt.Println("Listening for keyspace notifications... (Ctrl+C to stop)")
	} else {
		fmt.Printf("Replicating %s → %s (mode: %s, transfer: %s, match: %q)\n\n", cfg.sourceURI, cfg.targetURI, modeLabel, transferMode, cfg.match)
	}

	p := pipeline.New(reader, nil, writer,
		pipeline.WithQueueSize(Globals.QueueSize),
	)

	stats, err := p.Run(ctx)

	written := stats.RecordsWritten.Load()
	elapsed := stats.Elapsed()
	rps := float64(0)
	if elapsed > 0 {
		rps = float64(written) / elapsed.Seconds()
	}

	fmt.Printf("  records: %d\n", written)
	fmt.Printf("  elapsed: %s\n", elapsed.Truncate(time.Millisecond))
	fmt.Printf("  speed:   %.0f records/sec\n", rps)

	if err != nil && err != context.Canceled {
		return fmt.Errorf("replication failed: %w", err)
	}

	if cfg.migrateIndexes {
		fmt.Println("\nMigrating RediSearch indexes...")
		n, idxErr := redisclient.MigrateIndexes(ctx, srcClient, dstClient)
		fmt.Printf("  indexes migrated: %d\n", n)
		if idxErr != nil {
			fmt.Printf("  index migration warning: %v\n", idxErr)
		}
	}

	fmt.Println("\nDone.")
	return nil
}
