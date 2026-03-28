package transform

import (
	"context"
	"testing"
	"time"

	"github.com/adtyap26/redmove/internal/pipeline"
)

func TestExprProcessorFilter(t *testing.T) {
	proc, err := NewExprProcessor(TransformOpts{
		Filter: "score > 50",
	})
	if err != nil {
		t.Fatalf("NewExprProcessor: %v", err)
	}

	in := make(chan pipeline.Record, 3)
	out := make(chan pipeline.Record, 3)

	in <- pipeline.Record{Key: "a", Type: "hash", TTL: -1, Fields: map[string]any{"score": 100}}
	in <- pipeline.Record{Key: "b", Type: "hash", TTL: -1, Fields: map[string]any{"score": 30}}
	in <- pipeline.Record{Key: "c", Type: "hash", TTL: -1, Fields: map[string]any{"score": 75}}
	close(in)

	err = proc.Process(context.Background(), in, out)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	var results []string
	for rec := range out {
		results = append(results, rec.Key)
	}
	if len(results) != 2 || results[0] != "a" || results[1] != "c" {
		t.Errorf("expected [a, c], got %v", results)
	}
}

func TestExprProcessorTransform(t *testing.T) {
	proc, err := NewExprProcessor(TransformOpts{
		Transform: "score=score*2",
	})
	if err != nil {
		t.Fatalf("NewExprProcessor: %v", err)
	}

	in := make(chan pipeline.Record, 1)
	out := make(chan pipeline.Record, 1)

	in <- pipeline.Record{Key: "a", Type: "hash", TTL: -1, Fields: map[string]any{"score": 50}}
	close(in)

	err = proc.Process(context.Background(), in, out)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	rec := <-out
	score, ok := rec.Fields["score"]
	if !ok {
		t.Fatal("missing score field")
	}
	if score != 100 {
		t.Errorf("expected score=100, got %v", score)
	}
}

func TestExprProcessorKeyAccess(t *testing.T) {
	proc, err := NewExprProcessor(TransformOpts{
		Filter: "key startsWith \"user:\"",
	})
	if err != nil {
		t.Fatalf("NewExprProcessor: %v", err)
	}

	in := make(chan pipeline.Record, 2)
	out := make(chan pipeline.Record, 2)

	in <- pipeline.Record{Key: "user:1", Type: "hash", TTL: time.Hour, Fields: map[string]any{}}
	in <- pipeline.Record{Key: "order:1", Type: "hash", TTL: time.Hour, Fields: map[string]any{}}
	close(in)

	err = proc.Process(context.Background(), in, out)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	var results []string
	for rec := range out {
		results = append(results, rec.Key)
	}
	if len(results) != 1 || results[0] != "user:1" {
		t.Errorf("expected [user:1], got %v", results)
	}
}
