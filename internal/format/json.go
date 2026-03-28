package format

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/adtyap26/redmove/internal/pipeline"
)

func readJSON(ctx context.Context, path string, kt *KeyTemplate, recType string, out chan<- pipeline.Record) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open json: %w", err)
	}
	defer f.Close()

	var items []map[string]any
	if err := json.NewDecoder(f).Decode(&items); err != nil {
		return fmt.Errorf("decode json array: %w", err)
	}

	for _, item := range items {
		key, err := kt.Execute(item)
		if err != nil {
			return fmt.Errorf("generate key: %w", err)
		}

		rec := pipeline.Record{
			Key:    key,
			Type:   recType,
			TTL:    -1,
			Fields: item,
		}

		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func writeJSON(ctx context.Context, w io.Writer, in <-chan pipeline.Record) (int64, error) {
	var records []map[string]any
	var written int64

	for {
		select {
		case rec, ok := <-in:
			if !ok {
				// Write the collected array.
				enc := json.NewEncoder(w)
				enc.SetIndent("", "  ")
				if err := enc.Encode(records); err != nil {
					return written, err
				}
				return written, nil
			}

			entry := make(map[string]any, len(rec.Fields)+2)
			entry["_key"] = rec.Key
			entry["_type"] = rec.Type
			for k, v := range rec.Fields {
				entry[k] = v
			}
			records = append(records, entry)
			written++

		case <-ctx.Done():
			return written, ctx.Err()
		}
	}
}
