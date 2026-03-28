package redis

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/adtyap26/redmove/internal/pipeline"
	goredis "github.com/redis/go-redis/v9"
)

// RestoreOpts configures RestoreWriter.
type RestoreOpts struct {
	BatchSize int  // commands per Redis pipeline batch, default 200
	Replace   bool // use REPLACE flag on RESTORE
	Threads   int  // concurrent writer goroutines, default 4
}

// RestoreWriter writes Records via RESTORE command in batches.
type RestoreWriter struct {
	client goredis.UniversalClient
	opts   RestoreOpts
}

// NewRestoreWriter creates a RestoreWriter.
func NewRestoreWriter(client goredis.UniversalClient, opts RestoreOpts) *RestoreWriter {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 200
	}
	if opts.Threads <= 0 {
		opts.Threads = 4
	}
	return &RestoreWriter{client: client, opts: opts}
}

// Write implements pipeline.Writer.
func (w *RestoreWriter) Write(ctx context.Context, in <-chan pipeline.Record) (int64, error) {
	if w.opts.Threads <= 1 {
		return w.writeSingle(ctx, in)
	}
	return w.writeMulti(ctx, in)
}

// writeSingle runs one batch-flush loop, draining in until closed.
// Safe to call from multiple goroutines — each record is received by exactly one caller.
func (w *RestoreWriter) writeSingle(ctx context.Context, in <-chan pipeline.Record) (int64, error) {
	var written int64
	batch := make([]pipeline.Record, 0, w.opts.BatchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		pipe := w.client.Pipeline()
		for _, rec := range batch {
			ttlMs := int64(0)
			if rec.TTL > 0 {
				ttlMs = rec.TTL.Milliseconds()
			}
			args := []any{"RESTORE", rec.Key, ttlMs, string(rec.Raw)}
			if w.opts.Replace {
				args = append(args, "REPLACE")
			}
			pipe.Do(ctx, args...)
		}

		cmds, err := pipe.Exec(ctx)
		if err != nil {
			for i, cmd := range cmds {
				if cmd.Err() != nil {
					slog.Warn("restore failed", "key", batch[i].Key, "error", cmd.Err())
				} else {
					written++
				}
			}
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

// writeMulti fans out to Threads concurrent writeSingle goroutines.
func (w *RestoreWriter) writeMulti(ctx context.Context, in <-chan pipeline.Record) (int64, error) {
	var (
		total    atomic.Int64
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)

	for range w.opts.Threads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := w.writeSingle(ctx, in)
			total.Add(n)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return total.Load(), firstErr
}
