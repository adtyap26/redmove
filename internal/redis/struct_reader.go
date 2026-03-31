package redis

import (
	"context"
	"fmt"

	"github.com/adtyap26/redmove/internal/pipeline"
	goredis "github.com/redis/go-redis/v9"
)

// StructReader scans keys and reads their typed values into Fields.
type StructReader struct {
	client goredis.UniversalClient
	opts   ScanOpts
}

// NewStructReader creates a StructReader.
func NewStructReader(client goredis.UniversalClient, opts ScanOpts) *StructReader {
	opts.defaults()
	return &StructReader{client: client, opts: opts}
}

// Read implements pipeline.Reader.
func (s *StructReader) Read(ctx context.Context, out chan<- pipeline.Record) error {
	keyTypes := s.opts.KeyTypes
	return scanKeys(ctx, s.client, s.opts, func(ctx context.Context, client goredis.Cmdable, keys []string) ([]pipeline.Record, error) {
		return enrichStruct(ctx, client, keys, keyTypes)
	}, out)
}

// enrichStruct pipelines TYPE + PTTL then reads values per type.
func enrichStruct(ctx context.Context, client goredis.Cmdable, keys []string, keyTypes []string) ([]pipeline.Record, error) {
	// First: pipeline TYPE + PTTL.
	pipe := client.Pipeline()
	typeCmds := make([]*goredis.StatusCmd, len(keys))
	ttlCmds := make([]*goredis.DurationCmd, len(keys))

	for i, key := range keys {
		typeCmds[i] = pipe.Type(ctx, key)
		ttlCmds[i] = pipe.PTTL(ctx, key)
	}

	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, err
	}

	// Second: read values based on type.
	records := make([]pipeline.Record, 0, len(keys))
	for i, key := range keys {
		if typeCmds[i].Err() != nil {
			continue
		}
		typ := typeCmds[i].Val()
		if typ == "none" {
			continue
		}
		if !matchKeyType(typ, keyTypes) {
			continue
		}

		fields, err := ReadValue(ctx, client, key, typ)
		if err != nil {
			continue // key may have been deleted
		}

		rec := pipeline.Record{
			Key:    key,
			Type:   typ,
			Fields: fields,
		}
		if ttlCmds[i].Err() == nil {
			rec.TTL = ttlCmds[i].Val()
		}
		records = append(records, rec)
	}
	return records, nil
}

// ReadValue reads a key's value based on its Redis type and returns Fields.
func ReadValue(ctx context.Context, client goredis.Cmdable, key, typ string) (map[string]any, error) {
	switch typ {
	case "string":
		val, err := client.Get(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		return map[string]any{"value": val}, nil

	case "hash":
		val, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		fields := make(map[string]any, len(val))
		for k, v := range val {
			fields[k] = v
		}
		return fields, nil

	case "list":
		val, err := client.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		items := make([]any, len(val))
		for i, v := range val {
			items[i] = v
		}
		return map[string]any{"values": items}, nil

	case "set":
		val, err := client.SMembers(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		members := make([]any, len(val))
		for i, v := range val {
			members[i] = v
		}
		return map[string]any{"members": members}, nil

	case "zset":
		val, err := client.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		members := make([]any, len(val))
		for i, z := range val {
			members[i] = map[string]any{
				"member": z.Member,
				"score":  z.Score,
			}
		}
		return map[string]any{"members": members}, nil

	case "stream":
		val, err := client.XRange(ctx, key, "-", "+").Result()
		if err != nil {
			return nil, err
		}
		entries := make([]any, len(val))
		for i, msg := range val {
			fields := make(map[string]any, len(msg.Values))
			for k, v := range msg.Values {
				fields[k] = v
			}
			entries[i] = map[string]any{
				"id":     msg.ID,
				"fields": fields,
			}
		}
		return map[string]any{"entries": entries}, nil

	case "ReJSON-RL", "json":
		// RedisJSON module: read full document at root path.
		val, err := client.JSONGet(ctx, key, "$").Result()
		if err != nil {
			return nil, err
		}
		return map[string]any{"json": val}, nil

	case "TSDB-TYPE":
		// RedisTimeSeries module: read all samples.
		// Cmdable doesn't expose Do; assert to concrete doer interface.
		type doer interface {
			Do(ctx context.Context, args ...any) *goredis.Cmd
		}
		d, ok := client.(doer)
		if !ok {
			return nil, fmt.Errorf("redis client does not support raw commands for TimeSeries")
		}
		res, err := d.Do(ctx, "TS.RANGE", key, "-", "+").Result()
		if err != nil {
			return nil, err
		}
		rawSamples, ok := res.([]any)
		if !ok {
			return nil, fmt.Errorf("unexpected TS.RANGE result type")
		}
		samples := make([]any, 0, len(rawSamples))
		for _, s := range rawSamples {
			pair, ok := s.([]any)
			if !ok || len(pair) < 2 {
				continue
			}
			samples = append(samples, map[string]any{
				"ts":  pair[0],
				"val": pair[1],
			})
		}
		return map[string]any{"samples": samples}, nil

	default:
		return nil, fmt.Errorf("unsupported type: %s", typ)
	}
}
