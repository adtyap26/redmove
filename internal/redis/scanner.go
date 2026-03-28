package redis

import (
	"context"
	"log/slog"
	"path"
	"sync"

	"github.com/adtyap26/redmove/internal/pipeline"
	goredis "github.com/redis/go-redis/v9"
)

// matchKeyType returns true if typ is in the allowed list, or if allowed is empty.
func matchKeyType(typ string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, a := range allowed {
		if a == typ {
			return true
		}
	}
	return false
}

// matchPatterns returns true if key passes include/exclude filters.
// Include: key must match at least one pattern (empty = match all).
// Exclude: key must NOT match any pattern. Exclude takes precedence.
func matchPatterns(key string, include, exclude []string) bool {
	for _, pat := range exclude {
		if matched, _ := path.Match(pat, key); matched {
			return false
		}
	}
	if len(include) == 0 {
		return true
	}
	for _, pat := range include {
		if matched, _ := path.Match(pat, key); matched {
			return true
		}
	}
	return false
}

// filterKeysByPattern filters a slice of keys through include/exclude patterns.
func filterKeysByPattern(keys []string, include, exclude []string) []string {
	if len(include) == 0 && len(exclude) == 0 {
		return keys
	}
	filtered := make([]string, 0, len(keys))
	for _, k := range keys {
		if matchPatterns(k, include, exclude) {
			filtered = append(filtered, k)
		}
	}
	return filtered
}

// ScanOpts configures key scanning behavior.
type ScanOpts struct {
	Match     string   // SCAN MATCH pattern, default "*"
	Count     int64    // SCAN COUNT hint, default 1000
	BatchSize int      // keys to enrich per pipeline batch, default 100
	KeyTypes  []string // filter by Redis type (e.g. "hash","string"); empty = all types
	Include   []string // glob patterns — key must match at least one (empty = match all)
	Exclude   []string // glob patterns — key must NOT match any
	MemLimit  int64    // max key size in bytes (0 = no limit); only enforced in dump mode
}

func (o *ScanOpts) defaults() {
	if o.Match == "" {
		o.Match = "*"
	}
	if o.Count == 0 {
		o.Count = 1000
	}
	if o.BatchSize == 0 {
		o.BatchSize = 100
	}
}

// ScanReader iterates keys via SCAN, emitting Records with Key, Type, and TTL.
type ScanReader struct {
	client goredis.UniversalClient
	opts   ScanOpts
}

// NewScanReader creates a ScanReader.
func NewScanReader(client goredis.UniversalClient, opts ScanOpts) *ScanReader {
	opts.defaults()
	return &ScanReader{client: client, opts: opts}
}

// Read implements pipeline.Reader.
func (s *ScanReader) Read(ctx context.Context, out chan<- pipeline.Record) error {
	keyTypes := s.opts.KeyTypes
	return scanKeys(ctx, s.client, s.opts, func(ctx context.Context, client goredis.Cmdable, keys []string) ([]pipeline.Record, error) {
		return enrichTypeTTL(ctx, client, keys, keyTypes)
	}, out)
}

// enrichTypeTTL pipelines TYPE + PTTL for a batch of keys.
func enrichTypeTTL(ctx context.Context, client goredis.Cmdable, keys []string, keyTypes []string) ([]pipeline.Record, error) {
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

	records := make([]pipeline.Record, 0, len(keys))
	for i, key := range keys {
		rec := pipeline.Record{Key: key}
		if typeCmds[i].Err() == nil {
			rec.Type = typeCmds[i].Val()
		}
		if !matchKeyType(rec.Type, keyTypes) {
			continue
		}
		if ttlCmds[i].Err() == nil {
			rec.TTL = ttlCmds[i].Val()
		}
		records = append(records, rec)
	}
	return records, nil
}

// enrichFunc is the callback signature for scanKeys.
// It receives a batch of keys and returns enriched Records.
type enrichFunc func(ctx context.Context, client goredis.Cmdable, keys []string) ([]pipeline.Record, error)

// scanKeys is the shared SCAN iteration helper.
// It handles standalone vs cluster mode and calls fn for each batch of keys.
func scanKeys(
	ctx context.Context,
	client goredis.UniversalClient,
	opts ScanOpts,
	fn enrichFunc,
	out chan<- pipeline.Record,
) error {
	defer close(out)
	opts.defaults()

	// Cluster mode: scan each shard concurrently.
	if cc, ok := client.(*goredis.ClusterClient); ok {
		return scanCluster(ctx, cc, opts, fn, out)
	}

	// Standalone mode.
	return scanSingle(ctx, client, opts, fn, out)
}

func scanSingle(
	ctx context.Context,
	client goredis.Cmdable,
	opts ScanOpts,
	fn enrichFunc,
	out chan<- pipeline.Record,
) error {
	var cursor uint64
	for {
		keys, nextCursor, err := client.Scan(ctx, cursor, opts.Match, opts.Count).Result()
		if err != nil {
			return err
		}

		// Apply include/exclude pattern filtering before enrichment.
		keys = filterKeysByPattern(keys, opts.Include, opts.Exclude)

		// Process in sub-batches.
		for i := 0; i < len(keys); i += opts.BatchSize {
			end := i + opts.BatchSize
			if end > len(keys) {
				end = len(keys)
			}
			batch := keys[i:end]

			records, err := fn(ctx, client, batch)
			if err != nil {
				slog.Warn("enrich batch failed", "error", err)
				continue
			}

			for _, rec := range records {
				select {
				case out <- rec:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			return nil
		}
	}
}

func scanCluster(
	ctx context.Context,
	cc *goredis.ClusterClient,
	opts ScanOpts,
	fn enrichFunc,
	out chan<- pipeline.Record,
) error {
	// We need to manage channel closing ourselves since ForEachShard
	// runs fn concurrently but we already deferred close(out) in scanKeys.
	// So we use an intermediate channel per shard and merge.
	var mu sync.Mutex
	var firstErr error

	err := cc.ForEachShard(ctx, func(ctx context.Context, shard *goredis.Client) error {
		// Each shard scans independently, sending directly to out.
		// scanSingle does NOT close out — only scanKeys closes it via defer.
		var cursor uint64
		for {
			keys, nextCursor, err := shard.Scan(ctx, cursor, opts.Match, opts.Count).Result()
			if err != nil {
				return err
			}

			// Apply include/exclude pattern filtering before enrichment.
			keys = filterKeysByPattern(keys, opts.Include, opts.Exclude)

			for i := 0; i < len(keys); i += opts.BatchSize {
				end := i + opts.BatchSize
				if end > len(keys) {
					end = len(keys)
				}
				batch := keys[i:end]

				records, err := fn(ctx, shard, batch)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					slog.Warn("enrich batch failed on shard", "error", err)
					continue
				}

				for _, rec := range records {
					select {
					case out <- rec:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}

			cursor = nextCursor
			if cursor == 0 {
				return nil
			}
		}
	})

	if err != nil {
		return err
	}
	return firstErr
}
