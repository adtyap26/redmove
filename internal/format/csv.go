package format

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/adtyap26/redmove/internal/pipeline"
)

func readCSV(ctx context.Context, path string, kt *KeyTemplate, recType string, out chan<- pipeline.Record) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open csv: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)

	// First row is headers.
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read csv headers: %w", err)
	}

	for {
		row, err := reader.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read csv row: %w", err)
		}

		fields := make(map[string]any, len(headers))
		for i, h := range headers {
			if i < len(row) {
				fields[h] = row[i]
			}
		}

		key, err := kt.Execute(fields)
		if err != nil {
			return fmt.Errorf("generate key: %w", err)
		}

		rec := pipeline.Record{
			Key:    key,
			Type:   recType,
			TTL:    -1,
			Fields: fields,
		}

		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func writeCSV(ctx context.Context, w io.Writer, in <-chan pipeline.Record) (int64, error) {
	writer := csv.NewWriter(w)
	defer writer.Flush()

	var headers []string
	var written int64

	for {
		select {
		case rec, ok := <-in:
			if !ok {
				return written, nil
			}

			// Establish headers from first record.
			if headers == nil {
				headers = buildHeaders(rec)
				row := append([]string{"_key", "_type"}, headers...)
				if err := writer.Write(row); err != nil {
					return written, err
				}
			}

			row := make([]string, 0, len(headers)+2)
			row = append(row, rec.Key, rec.Type)
			for _, h := range headers {
				val, ok := rec.Fields[h]
				if ok {
					row = append(row, fmt.Sprintf("%v", val))
				} else {
					row = append(row, "")
				}
			}
			if err := writer.Write(row); err != nil {
				return written, err
			}
			written++

		case <-ctx.Done():
			return written, ctx.Err()
		}
	}
}

func buildHeaders(rec pipeline.Record) []string {
	// For hash: use field keys. For others: use Fields keys.
	headers := make([]string, 0, len(rec.Fields))
	for k := range rec.Fields {
		headers = append(headers, k)
	}
	sort.Strings(headers)
	return headers
}
