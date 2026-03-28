package redis

import (
	"context"
	"log/slog"

	"github.com/adtyap26/redmove/internal/pipeline"
	goredis "github.com/redis/go-redis/v9"
)

// DumpReader scans keys and reads their DUMP payload, Type, and TTL.
type DumpReader struct {
	client goredis.UniversalClient
	opts   ScanOpts
}

// NewDumpReader creates a DumpReader.
func NewDumpReader(client goredis.UniversalClient, opts ScanOpts) *DumpReader {
	opts.defaults()
	return &DumpReader{client: client, opts: opts}
}

// Read implements pipeline.Reader.
func (d *DumpReader) Read(ctx context.Context, out chan<- pipeline.Record) error {
	keyTypes := d.opts.KeyTypes
	memLimit := d.opts.MemLimit
	return scanKeys(ctx, d.client, d.opts, func(ctx context.Context, client goredis.Cmdable, keys []string) ([]pipeline.Record, error) {
		return enrichDump(ctx, client, keys, keyTypes, memLimit)
	}, out)
}

// enrichDump pipelines DUMP + TYPE + PTTL for a batch of keys.
func enrichDump(ctx context.Context, client goredis.Cmdable, keys []string, keyTypes []string, memLimit int64) ([]pipeline.Record, error) {
	pipe := client.Pipeline()
	dumpCmds := make([]*goredis.StringCmd, len(keys))
	typeCmds := make([]*goredis.StatusCmd, len(keys))
	ttlCmds := make([]*goredis.DurationCmd, len(keys))

	for i, key := range keys {
		dumpCmds[i] = pipe.Dump(ctx, key)
		typeCmds[i] = pipe.Type(ctx, key)
		ttlCmds[i] = pipe.PTTL(ctx, key)
	}

	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, err
	}

	records := make([]pipeline.Record, 0, len(keys))
	for i, key := range keys {
		if dumpCmds[i].Err() != nil {
			continue // key may have been deleted between SCAN and DUMP
		}
		typ := ""
		if typeCmds[i].Err() == nil {
			typ = typeCmds[i].Val()
		}
		if !matchKeyType(typ, keyTypes) {
			continue
		}
		raw := []byte(dumpCmds[i].Val())
		if memLimit > 0 && int64(len(raw)) > memLimit {
			slog.Warn("skipping big key", "key", key, "size", len(raw), "limit", memLimit)
			continue
		}
		rec := pipeline.Record{
			Key:  key,
			Type: typ,
			Raw:  raw,
		}
		if ttlCmds[i].Err() == nil {
			rec.TTL = ttlCmds[i].Val()
		}
		records = append(records, rec)
	}
	return records, nil
}
