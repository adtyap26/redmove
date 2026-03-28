package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/adtyap26/redmove/internal/format"
	"github.com/adtyap26/redmove/internal/pipeline"
	redisclient "github.com/adtyap26/redmove/internal/redis"
	"github.com/spf13/cobra"
)

func newImportCmd() *cobra.Command {
	var (
		file        string
		keyTemplate string
		recType     string
	)

	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import data from file (CSV/JSON/JSONL) into Redis",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runImport(cmd.Context(), file, keyTemplate, recType)
		},
	}

	f := cmd.Flags()
	f.StringVar(&file, "file", "", "Input file path (required)")
	f.StringVar(&keyTemplate, "key-template", "", `Key template, e.g. "user:#{id}" (required)`)
	f.StringVar(&recType, "type", "hash", "Target Redis type: string, hash, list, set, zset")

	cmd.MarkFlagRequired("file")
	cmd.MarkFlagRequired("key-template")

	return cmd
}

func runImport(ctx context.Context, file, keyTemplate, recType string) error {
	client, err := redisclient.NewClient(OptsFromGlobals())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	reader, err := format.NewFileReader(format.FileReaderOpts{
		Path:        file,
		KeyTemplate: keyTemplate,
		RecordType:  recType,
	})
	if err != nil {
		return err
	}

	writer := redisclient.NewStructWriter(client, redisclient.StructWriteOpts{
		BatchSize: Globals.BatchSize,
	})

	fmt.Printf("Importing %s → %s (type: %s)\n\n", file, Globals.URI, recType)

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

	if err != nil {
		return fmt.Errorf("import failed: %w", err)
	}
	fmt.Println("\nDone.")
	return nil
}
