package bucket

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var keyFormat = "dispatch_bucket_%d_%d"

// bucket is an implement of Bucket.
type bucket struct {
	mx sync.RWMutex

	key        string
	expiration int64
	backend    *redis.ClusterClient
}

type FuncOption func(*bucket)

func WithExpiration(expiration int64) FuncOption {
	return func(b *bucket) {
		b.expiration = expiration
	}
}

// Index return ths bucket key in redis.
func Index(level, index int64) string {
	return fmt.Sprintf(keyFormat, level, index)
}

// New only use for flower process task from bucket.
func New(rdb *redis.ClusterClient, index string, options ...FuncOption) Bucket {
	b := &bucket{
		key:     index,
		backend: rdb,
	}
	for _, option := range options {
		option(b)
	}
	return b
}

// Add task to the bucket, only record task id use for index.
func (b *bucket) Add(ctx context.Context, value string) error {
	return b.backend.RPush(ctx, b.key, value).Err()
}

// Flush only use for process task from bucket.
func (b *bucket) Flush(callback func(value string)) {
	for {
		bRPop, err := b.backend.BLPop(context.Background(), time.Second, b.key).Result()
		if err == redis.Nil {
			// If no data arrives before the timeout expires that means there is no data in the bucket, then exit consumer.
			break
		}
		if err != nil {
			continue
		}

		callback(bRPop[1])
	}
}

// Expiration return this bucket`s expiration.
func (b *bucket) Expiration() int64 {
	b.mx.RLock()
	defer b.mx.RUnlock()
	return b.expiration
}

// SetExpiration set the bucket expiration.
func (b *bucket) SetExpiration(expiration int64) bool {
	b.mx.Lock()
	defer b.mx.Unlock()
	// first init b.expiration = 0
	if b.expiration == 0 || expiration < b.expiration {
		b.expiration = expiration
		return true
	}
	return false
}

// Index return this bucket key in redis.
func (b *bucket) Index() string {
	return b.key
}
