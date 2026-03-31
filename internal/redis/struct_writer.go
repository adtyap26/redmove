package redis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/adtyap26/redmove/internal/pipeline"
	goredis "github.com/redis/go-redis/v9"
)

// StructWriteOpts configures StructWriter.
type StructWriteOpts struct {
	BatchSize    int    // commands per Redis pipeline batch, default 50
	StreamIDMode string // "preserve" (default) or "reset" for stream entry IDs
}

// StructWriter writes Records via type-specific commands in batches.
type StructWriter struct {
	client goredis.UniversalClient
	opts   StructWriteOpts
}

// NewStructWriter creates a StructWriter.
func NewStructWriter(client goredis.UniversalClient, opts StructWriteOpts) *StructWriter {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 50
	}
	return &StructWriter{client: client, opts: opts}
}

// Write implements pipeline.Writer.
func (w *StructWriter) Write(ctx context.Context, in <-chan pipeline.Record) (int64, error) {
	var written int64
	batch := make([]pipeline.Record, 0, w.opts.BatchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		pipe := w.client.Pipeline()
		for _, rec := range batch {
			if err := writeRecord(ctx, pipe, rec, w.opts.StreamIDMode); err != nil {
				slog.Warn("skip record", "key", rec.Key, "error", err)
				continue
			}
			if rec.TTL > 0 {
				pipe.PExpire(ctx, rec.Key, rec.TTL)
			}
		}

		cmds, err := pipe.Exec(ctx)
		if err != nil {
			// Count individual successes.
			successes := int64(0)
			for _, cmd := range cmds {
				if cmd.Err() == nil {
					successes++
				}
			}
			// Each record produces 1-2 commands (write + optional TTL).
			// Approximate: count non-error commands.
			written += successes
		} else {
			written += int64(len(batch))
		}
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case rec, ok := <-in:
			if !ok {
				return written, flush()
			}
			batch = append(batch, rec)
			if len(batch) >= w.opts.BatchSize {
				if err := flush(); err != nil {
					return written, err
				}
			}
		case <-ctx.Done():
			return written, ctx.Err()
		}
	}
}

// writeRecord adds the appropriate write command(s) to the pipeline.
func writeRecord(ctx context.Context, pipe goredis.Pipeliner, rec pipeline.Record, streamIDMode string) error {
	switch rec.Type {
	case "string":
		val, ok := rec.Fields["value"]
		if !ok {
			return fmt.Errorf("string record missing 'value' field")
		}
		pipe.Set(ctx, rec.Key, val, 0)

	case "hash":
		if len(rec.Fields) == 0 {
			return fmt.Errorf("hash record has no fields")
		}
		pipe.HSet(ctx, rec.Key, rec.Fields)

	case "list":
		vals, ok := rec.Fields["values"].([]any)
		if !ok || len(vals) == 0 {
			return fmt.Errorf("list record missing 'values' field")
		}
		pipe.RPush(ctx, rec.Key, vals...)

	case "set":
		members, ok := rec.Fields["members"].([]any)
		if !ok || len(members) == 0 {
			return fmt.Errorf("set record missing 'members' field")
		}
		pipe.SAdd(ctx, rec.Key, members...)

	case "zset":
		members, ok := rec.Fields["members"].([]any)
		if !ok || len(members) == 0 {
			return fmt.Errorf("zset record missing 'members' field")
		}
		zMembers := make([]goredis.Z, 0, len(members))
		for _, m := range members {
			mm, ok := m.(map[string]any)
			if !ok {
				continue
			}
			score, _ := mm["score"].(float64)
			zMembers = append(zMembers, goredis.Z{
				Score:  score,
				Member: mm["member"],
			})
		}
		if len(zMembers) == 0 {
			return fmt.Errorf("zset record has no valid members")
		}
		pipe.ZAdd(ctx, rec.Key, zMembers...)

	case "stream":
		entries, ok := rec.Fields["entries"].([]any)
		if !ok || len(entries) == 0 {
			return fmt.Errorf("stream record missing 'entries' field")
		}
		for _, entry := range entries {
			e, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			id := "*"
			if streamIDMode != "reset" {
				if eid, ok := e["id"].(string); ok && eid != "" {
					id = eid
				}
			}
			fields, ok := e["fields"].(map[string]any)
			if !ok {
				continue
			}
			values := make(map[string]any, len(fields))
			for k, v := range fields {
				values[k] = v
			}
			pipe.XAdd(ctx, &goredis.XAddArgs{
				Stream: rec.Key,
				ID:     id,
				Values: values,
			})
		}

	case "ReJSON-RL", "json":
		// RedisJSON module: write full document at root path.
		val, ok := rec.Fields["json"]
		if !ok {
			return fmt.Errorf("json record missing 'json' field")
		}
		pipe.JSONSet(ctx, rec.Key, "$", val)

	case "TSDB-TYPE":
		// RedisTimeSeries module: create key then add each sample.
		samples, ok := rec.Fields["samples"].([]any)
		if !ok || len(samples) == 0 {
			return fmt.Errorf("timeseries record missing 'samples' field")
		}
		// Create the TS key first (ignore error if already exists).
		pipe.Do(ctx, "TS.CREATE", rec.Key, "DUPLICATE_POLICY", "LAST")
		for _, s := range samples {
			sample, ok := s.(map[string]any)
			if !ok {
				continue
			}
			pipe.Do(ctx, "TS.ADD", rec.Key, sample["ts"], sample["val"])
		}

	default:
		return fmt.Errorf("unsupported type: %s", rec.Type)
	}
	return nil
}
