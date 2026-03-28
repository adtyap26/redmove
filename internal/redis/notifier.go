package redis

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/adtyap26/redmove/internal/pipeline"
	goredis "github.com/redis/go-redis/v9"
)

// NotifyOpts configures the NotificationReader.
type NotifyOpts struct {
	DB        int      // database number for the subscription channel
	Match     string   // optional key pattern filter (client-side)
	UseStruct bool     // if true, use ReadValue; if false, use DUMP
	KeyTypes  []string // filter by Redis type; empty = all types
	Include   []string // glob patterns — key must match at least one (empty = match all)
	Exclude   []string // glob patterns — key must NOT match any
}

// NotificationReader subscribes to keyspace notifications and emits Records.
type NotificationReader struct {
	client goredis.UniversalClient
	opts   NotifyOpts
}

// NewNotificationReader creates a NotificationReader.
func NewNotificationReader(client goredis.UniversalClient, opts NotifyOpts) *NotificationReader {
	return &NotificationReader{client: client, opts: opts}
}

// Read implements pipeline.Reader. It blocks until ctx is cancelled.
func (n *NotificationReader) Read(ctx context.Context, out chan<- pipeline.Record) error {
	defer close(out)

	// Try to enable keyspace notifications.
	if err := n.client.ConfigSet(ctx, "notify-keyspace-events", "KEA").Err(); err != nil {
		slog.Warn("could not enable keyspace notifications (may need manual CONFIG SET)", "error", err)
	}

	channel := fmt.Sprintf("__keyevent@%d__:*", n.opts.DB)

	// Cluster mode: subscribe on each shard.
	if cc, ok := n.client.(*goredis.ClusterClient); ok {
		return n.subscribeCluster(ctx, cc, channel, out)
	}

	// Standalone mode.
	return n.subscribeSingle(ctx, n.client, channel, out)
}

func (n *NotificationReader) subscribeSingle(ctx context.Context, client goredis.UniversalClient, channel string, out chan<- pipeline.Record) error {
	pubsub := client.PSubscribe(ctx, channel)
	defer pubsub.Close()

	ch := pubsub.Channel()
	dedup := newDedupMap()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			rec, ok := n.processMessage(ctx, client, msg, dedup)
			if !ok {
				continue
			}
			select {
			case out <- rec:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (n *NotificationReader) subscribeCluster(ctx context.Context, cc *goredis.ClusterClient, channel string, out chan<- pipeline.Record) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	err := cc.ForEachShard(ctx, func(ctx context.Context, shard *goredis.Client) error {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := n.subscribeSingle(ctx, shard, channel, out); err != nil && err != context.Canceled {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
		return nil
	})
	if err != nil {
		return err
	}

	wg.Wait()
	return firstErr
}

func (n *NotificationReader) processMessage(ctx context.Context, client goredis.UniversalClient, msg *goredis.Message, dedup *dedupMap) (pipeline.Record, bool) {
	key := msg.Payload

	// Client-side pattern filter.
	if n.opts.Match != "" && n.opts.Match != "*" {
		matched, _ := filepath.Match(n.opts.Match, key)
		if !matched {
			return pipeline.Record{}, false
		}
	}

	// Include/exclude pattern filter.
	if !matchPatterns(key, n.opts.Include, n.opts.Exclude) {
		return pipeline.Record{}, false
	}

	// Dedup: skip if emitted recently.
	if !dedup.shouldProcess(key) {
		return pipeline.Record{}, false
	}

	// Extract event type from channel name.
	// Channel format: __keyevent@0__:set
	event := ""
	if idx := strings.LastIndex(msg.Channel, ":"); idx != -1 {
		event = msg.Channel[idx+1:]
	}

	// Handle delete/expire events.
	if event == "del" || event == "expired" || event == "evicted" {
		return pipeline.Record{
			Key:  key,
			Type: "del",
			TTL:  -1,
		}, true
	}

	// Read current state.
	if n.opts.UseStruct {
		typ, err := client.Type(ctx, key).Result()
		if err != nil || typ == "none" {
			return pipeline.Record{}, false
		}
		if !matchKeyType(typ, n.opts.KeyTypes) {
			return pipeline.Record{}, false
		}
		fields, err := ReadValue(ctx, client, key, typ)
		if err != nil {
			return pipeline.Record{}, false
		}
		ttl, _ := client.PTTL(ctx, key).Result()
		return pipeline.Record{
			Key:    key,
			Type:   typ,
			TTL:    ttl,
			Fields: fields,
		}, true
	}

	// DUMP mode.
	pipe := client.Pipeline()
	dumpCmd := pipe.Dump(ctx, key)
	typeCmd := pipe.Type(ctx, key)
	ttlCmd := pipe.PTTL(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return pipeline.Record{}, false
	}
	if dumpCmd.Err() != nil {
		return pipeline.Record{}, false
	}
	if !matchKeyType(typeCmd.Val(), n.opts.KeyTypes) {
		return pipeline.Record{}, false
	}

	return pipeline.Record{
		Key:  key,
		Type: typeCmd.Val(),
		TTL:  ttlCmd.Val(),
		Raw:  []byte(dumpCmd.Val()),
	}, true
}

// dedupMap prevents emitting the same key multiple times within a short window.
type dedupMap struct {
	mu      sync.Mutex
	keys    map[string]time.Time
	cleanup time.Time
}

func newDedupMap() *dedupMap {
	return &dedupMap{
		keys:    make(map[string]time.Time),
		cleanup: time.Now().Add(10 * time.Second),
	}
}

func (d *dedupMap) shouldProcess(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()

	// Periodic cleanup.
	if now.After(d.cleanup) {
		cutoff := now.Add(-time.Second)
		for k, t := range d.keys {
			if t.Before(cutoff) {
				delete(d.keys, k)
			}
		}
		d.cleanup = now.Add(10 * time.Second)
	}

	if last, ok := d.keys[key]; ok && now.Sub(last) < 50*time.Millisecond {
		return false
	}
	d.keys[key] = now
	return true
}
