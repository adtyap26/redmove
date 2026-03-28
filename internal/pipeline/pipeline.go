package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Stats tracks pipeline execution metrics.
type Stats struct {
	RecordsRead     atomic.Int64
	RecordsWritten  atomic.Int64
	RecordsFiltered atomic.Int64
	Errors          atomic.Int64
	StartTime       time.Time
	EndTime         time.Time
}

// LiveStats returns a pointer to the pipeline's statistics.
// Safe to read from concurrent goroutines — numeric fields use atomic operations.
func (p *Pipeline) LiveStats() *Stats {
	return &p.stats
}

// Elapsed returns the pipeline duration.
func (s *Stats) Elapsed() time.Duration {
	end := s.EndTime
	if end.IsZero() {
		end = time.Now()
	}
	return end.Sub(s.StartTime)
}

// Pipeline orchestrates Reader → Processor → Writer with channels.
type Pipeline struct {
	reader    Reader
	processor Processor
	writer    Writer
	queueSize int
	stats     Stats
	logger    *slog.Logger
}

// Option configures a Pipeline.
type Option func(*Pipeline)

// WithQueueSize sets the internal channel buffer size.
func WithQueueSize(n int) Option {
	return func(p *Pipeline) { p.queueSize = n }
}

// WithLogger sets the logger for pipeline operations.
func WithLogger(l *slog.Logger) Option {
	return func(p *Pipeline) { p.logger = l }
}

// New creates a Pipeline. If processor is nil, PassthroughProcessor is used.
func New(r Reader, proc Processor, w Writer, opts ...Option) *Pipeline {
	if proc == nil {
		proc = &PassthroughProcessor{}
	}
	p := &Pipeline{
		reader:    r,
		processor: proc,
		writer:    w,
		queueSize: 10000,
		logger:    slog.Default(),
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Run executes the pipeline. It blocks until completion or context cancellation.
func (p *Pipeline) Run(ctx context.Context) (Stats, error) {
	p.stats.StartTime = time.Now()
	defer func() { p.stats.EndTime = time.Now() }()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	readerOut := make(chan Record, p.queueSize)
	processorOut := make(chan Record, p.queueSize)

	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	setErr := func(err error) {
		if err != nil {
			errOnce.Do(func() {
				firstErr = err
				cancel(err)
			})
		}
	}

	// Start reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.reader.Read(ctx, readerOut); err != nil {
			p.logger.Error("reader failed", "error", err)
			setErr(err)
		}
	}()

	// Start processor
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.processor.Process(ctx, readerOut, processorOut); err != nil {
			p.logger.Error("processor failed", "error", err)
			setErr(err)
		}
	}()

	// Intercept processorOut to count records in real time, before the writer sees them.
	counted := make(chan Record, p.queueSize)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(counted)
		for rec := range processorOut {
			p.stats.RecordsWritten.Add(1)
			select {
			case counted <- rec:
			case <-ctx.Done():
				for range processorOut {} // drain so processor can unblock
				return
			}
		}
	}()

	// Start writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := p.writer.Write(ctx, counted)
		if err != nil {
			p.logger.Error("writer failed", "error", err)
			setErr(err)
		}
	}()

	wg.Wait()
	return p.stats, firstErr
}
