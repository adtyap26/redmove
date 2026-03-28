package pipeline

import "context"

// Writer consumes records from the input channel.
// It returns the total number of records written and any terminal error.
type Writer interface {
	Write(ctx context.Context, in <-chan Record) (written int64, err error)
}
