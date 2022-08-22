package mq

import (
	"context"
)

type Producer interface {
	Product(ctx context.Context, value []byte) error
	Close()
}

type Consumer interface {
	Consume(exit chan struct{}, callback func(value []byte) error)
}
