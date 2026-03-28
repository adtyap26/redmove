package pipeline

import "context"

// Processor transforms or filters records.
// It reads from in, processes each record, and sends results to out.
// The processor MUST close the out channel when the in channel is closed.
type Processor interface {
	Process(ctx context.Context, in <-chan Record, out chan<- Record) error
}

// PassthroughProcessor is a no-op processor that forwards records unchanged.
type PassthroughProcessor struct{}

func (p *PassthroughProcessor) Process(ctx context.Context, in <-chan Record, out chan<- Record) error {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rec, ok := <-in:
			if !ok {
				return nil
			}
			select {
			case out <- rec:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
