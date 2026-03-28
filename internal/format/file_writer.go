package format

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/adtyap26/redmove/internal/pipeline"
)

// FileWriterOpts configures FileWriter.
type FileWriterOpts struct {
	Path   string // output file path; empty means stdout
	Format Format
}

// FileWriter writes records to a file or stdout.
type FileWriter struct {
	opts FileWriterOpts
}

// NewFileWriter creates a FileWriter.
func NewFileWriter(opts FileWriterOpts) *FileWriter {
	return &FileWriter{opts: opts}
}

// Write implements pipeline.Writer.
func (fw *FileWriter) Write(ctx context.Context, in <-chan pipeline.Record) (int64, error) {
	var w io.Writer
	if fw.opts.Path != "" {
		f, err := os.Create(fw.opts.Path)
		if err != nil {
			return 0, fmt.Errorf("create output file: %w", err)
		}
		defer f.Close()
		w = f
	} else {
		w = os.Stdout
	}

	switch fw.opts.Format {
	case FormatCSV:
		return writeCSV(ctx, w, in)
	case FormatJSON:
		return writeJSON(ctx, w, in)
	case FormatJSONL:
		return writeJSONL(ctx, w, in)
	case FormatXML:
		return writeXML(ctx, w, in)
	default:
		return 0, fmt.Errorf("unsupported format: %v", fw.opts.Format)
	}
}
