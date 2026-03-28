package pipeline

import (
	"context"
	"fmt"
	"testing"
)

// mockReader emits n records then closes the channel.
type mockReader struct {
	n int
}

func (r *mockReader) Read(ctx context.Context, out chan<- Record) error {
	defer close(out)
	for i := range r.n {
		rec := Record{
			Key:  fmt.Sprintf("key:%d", i),
			Type: "string",
		}
		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// mockWriter counts records received.
type mockWriter struct{}

func (w *mockWriter) Write(ctx context.Context, in <-chan Record) (int64, error) {
	var count int64
	for {
		select {
		case _, ok := <-in:
			if !ok {
				return count, nil
			}
			count++
		case <-ctx.Done():
			return count, ctx.Err()
		}
	}
}

func TestPipelinePassthrough(t *testing.T) {
	const n = 100
	p := New(&mockReader{n: n}, nil, &mockWriter{}, WithQueueSize(16))

	stats, err := p.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := stats.RecordsWritten.Load(); got != n {
		t.Errorf("RecordsWritten = %d, want %d", got, n)
	}
	if stats.Elapsed() <= 0 {
		t.Error("Elapsed should be positive")
	}
}

func TestPipelineCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	p := New(&mockReader{n: 1000}, nil, &mockWriter{}, WithQueueSize(1))

	_, err := p.Run(ctx)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}
