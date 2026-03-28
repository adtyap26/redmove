package pipeline

import "context"

// Reader produces records and sends them to the output channel.
// The reader MUST close the out channel when finished or when ctx is cancelled.
type Reader interface {
	Read(ctx context.Context, out chan<- Record) error
}
