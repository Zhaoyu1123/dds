package timingqueue

import "context"

// TimingQueue represents a queue sorted by timestamp.
type TimingQueue interface {
	// Push add value to TimingQueue.
	Push(ctx context.Context, timestamp int64, value string) error

	// BlockPop waiting for element expire of TimingQueue.
	BlockPop() <-chan Element

	// Timestamp get the expiration time of an element in the TimingQueue.
	Timestamp(ctx context.Context, value string) (int64, error)
}

// Element store in TimingQueue.
type Element struct {
	Value     string
	Timestamp int64
}
