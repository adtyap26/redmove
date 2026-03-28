package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/adtyap26/redmove/internal/generate"
	"github.com/adtyap26/redmove/internal/pipeline"
	redisclient "github.com/adtyap26/redmove/internal/redis"
	"github.com/spf13/cobra"
)

func newGenerateCmd() *cobra.Command {
	var (
		count       int
		keyTemplate string
		fields      string
		recType     string
	)

	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate synthetic data into Redis",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGenerate(cmd.Context(), count, keyTemplate, fields, recType)
		},
	}

	f := cmd.Flags()
	f.IntVar(&count, "count", 0, "Number of records to generate (required)")
	f.StringVar(&keyTemplate, "key-template", "", `Key template, e.g. "user:#{seq}" (required)`)
	f.StringVar(&fields, "fields", "", `Field specs, e.g. "name:name,email:email,age:number" (required)`)
	f.StringVar(&recType, "type", "hash", "Target Redis type: string, hash, list, set, zset")

	cmd.MarkFlagRequired("count")
	cmd.MarkFlagRequired("key-template")
	cmd.MarkFlagRequired("fields")

	return cmd
}

func runGenerate(ctx context.Context, count int, keyTemplate, fields, recType string) error {
	client, err := redisclient.NewClient(OptsFromGlobals())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	reader, err := generate.NewFakerReader(generate.FakerOpts{
		Count:       count,
		KeyTemplate: keyTemplate,
		Fields:      fields,
		RecordType:  recType,
	})
	if err != nil {
		return err
	}

	writer := redisclient.NewStructWriter(client, redisclient.StructWriteOpts{
		BatchSize: Globals.BatchSize,
	})

	uri := Globals.URI
	if uri == "" {
		uri = "redis://localhost:6379"
	}
	fmt.Printf("Generating %d records → %s (type: %s)\n\n", count, uri, recType)

	p := pipeline.New(reader, nil, writer,
		pipeline.WithQueueSize(Globals.QueueSize),
	)

	stats, runErr := p.Run(ctx)

	written := stats.RecordsWritten.Load()
	elapsed := stats.Elapsed()
	rps := float64(0)
	if elapsed > 0 {
		rps = float64(written) / elapsed.Seconds()
	}

	fmt.Printf("  records: %d\n", written)
	fmt.Printf("  elapsed: %s\n", elapsed.Truncate(time.Millisecond))
	fmt.Printf("  speed:   %.0f records/sec\n", rps)

	if runErr != nil {
		return fmt.Errorf("generate failed: %w", runErr)
	}
	fmt.Println("\nDone.")
	return nil
}
