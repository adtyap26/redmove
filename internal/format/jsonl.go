package format

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/adtyap26/redmove/internal/pipeline"
)

func readJSONL(ctx context.Context, path string, kt *KeyTemplate, recType string, out chan<- pipeline.Record) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open jsonl: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024) // 10MB max line

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var item map[string]any
		if err := json.Unmarshal(line, &item); err != nil {
			return fmt.Errorf("decode jsonl line: %w", err)
		}

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
	return scanner.Err()
}

func writeJSONL(ctx context.Context, w io.Writer, in <-chan pipeline.Record) (int64, error) {
	enc := json.NewEncoder(w)
	var written int64

	for {
		select {
		case rec, ok := <-in:
			if !ok {
				return written, nil
			}

			entry := make(map[string]any, len(rec.Fields)+2)
			entry["_key"] = rec.Key
			entry["_type"] = rec.Type
			for k, v := range rec.Fields {
				entry[k] = v
			}
			if err := enc.Encode(entry); err != nil {
				return written, err
			}
			written++

		case <-ctx.Done():
			return written, ctx.Err()
		}
	}
}
