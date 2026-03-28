package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/adtyap26/redmove/internal/format"
	"github.com/adtyap26/redmove/internal/pipeline"
	redisclient "github.com/adtyap26/redmove/internal/redis"
	"github.com/spf13/cobra"
)

func newExportCmd() *cobra.Command {
	var (
		match      string
		formatStr  string
		output     string
		keyTypes   []string
		keyInclude []string
		keyExclude []string
	)

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export Redis keys to file (CSV/JSON/JSONL/XML)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExport(cmd.Context(), exportConfig{
				match:      match,
				formatStr:  formatStr,
				output:     output,
				keyTypes:   keyTypes,
				keyInclude: keyInclude,
				keyExclude: keyExclude,
			})
		},
	}

	f := cmd.Flags()
	f.StringVar(&match, "match", "*", "Key pattern to export")
	f.StringVar(&formatStr, "format", "jsonl", "Output format: csv, json, jsonl, xml")
	f.StringVar(&output, "output", "", "Output file path (default: stdout)")
	f.StringSliceVar(&keyTypes, "key-type", nil, "Filter by Redis type (e.g. hash,string)")
	f.StringSliceVar(&keyInclude, "key-include", nil, "Include keys matching glob pattern (repeatable)")
	f.StringSliceVar(&keyExclude, "key-exclude", nil, "Exclude keys matching glob pattern (repeatable)")

	return cmd
}

type exportConfig struct {
	match      string
	formatStr  string
	output     string
	keyTypes   []string
	keyInclude []string
	keyExclude []string
}

func runExport(ctx context.Context, cfg exportConfig) error {
	client, err := redisclient.NewClient(OptsFromGlobals())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()

	outFmt, err := format.ParseFormat(cfg.formatStr)
	if err != nil {
		return err
	}

	reader := redisclient.NewStructReader(client, redisclient.ScanOpts{
		Match:     cfg.match,
		Count:     1000,
		BatchSize: Globals.BatchSize,
		KeyTypes:  cfg.keyTypes,
		Include:   cfg.keyInclude,
		Exclude:   cfg.keyExclude,
	})

	writer := format.NewFileWriter(format.FileWriterOpts{
		Path:   cfg.output,
		Format: outFmt,
	})

	dest := cfg.output
	if dest == "" {
		dest = "stdout"
	}

	// Print status to stderr so it doesn't mix with stdout data output.
	printStatus := cfg.output != ""
	if printStatus {
		fmt.Fprintf(os.Stderr, "Exporting %s → %s (format: %s, match: %q)\n\n", Globals.URI, dest, cfg.formatStr, cfg.match)
	}

	p := pipeline.New(reader, nil, writer,
		pipeline.WithQueueSize(Globals.QueueSize),
	)

	stats, runErr := p.Run(ctx)

	if printStatus {
		written := stats.RecordsWritten.Load()
		elapsed := stats.Elapsed()
		rps := float64(0)
		if elapsed > 0 {
			rps = float64(written) / elapsed.Seconds()
		}
		fmt.Fprintf(os.Stderr, "  records: %d\n", written)
		fmt.Fprintf(os.Stderr, "  elapsed: %s\n", elapsed.Truncate(time.Millisecond))
		fmt.Fprintf(os.Stderr, "  speed:   %.0f records/sec\n", rps)
	}

	if runErr != nil {
		return fmt.Errorf("export failed: %w", runErr)
	}
	if printStatus {
		fmt.Fprintln(os.Stderr, "\nDone.")
	}
	return nil
}
