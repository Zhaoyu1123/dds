package timingqueue

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

const maxDuration = time.Duration(1<<63 - 1)

var key = "dispatch_timing_queue"

// queue implement TimingQueue
type queue struct {
	// cache the min heap top element to reduce redis query operations.
	heapTop *Element

	nextC chan Element

	timer *time.Timer

	logger *zap.Logger

	backend *redis.ClusterClient
}

// New return an implement of TimingQueue.
func New(exit chan struct{}, backend *redis.ClusterClient, logger *zap.Logger) (TimingQueue, error) {
	q := &queue{
		logger:  logger,
		backend: backend,
		nextC:   make(chan Element),

		// set the next trigger time of timer to 0, and drain the expired elements in the queue.
		timer: time.NewTimer(0),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := q.backend.ZRangeWithScores(ctx, key, 0, 0).Result()
	if err != nil {
		return nil, err
	}

	if len(result) != 0 {
		q.heapTop = &Element{
			Value:     result[0].Member.(string),
			Timestamp: int64(result[0].Score),
		}
	}

	go q.start(exit)
	return q, nil
}

// start waiting for queue element expire or context done.
func (t *queue) start(exit chan struct{}) {
	t.logger.Info("[TimingQueue] start, waiting for element to expire")
	var ctx context.Context
	var cancel context.CancelFunc

next:
	for {
		if cancel != nil {
			cancel()
		}

		select {
		case <-t.timer.C:
			ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
			if t.heapTop != nil {
				t.logger.Info("[TimingQue] execute expire operation", zap.Any("element", t.heapTop))
				t.nextC <- *t.heapTop
				if err := t.backend.ZRem(ctx, key, t.heapTop.Value).Err(); err != nil {
					t.logger.Error("[TimingQueue] execute expire operation, redis ZRem", zap.Error(err))
				}
				t.heapTop = nil
			}

			result, err := t.backend.ZRangeWithScores(ctx, key, 0, 0).Result()
			if err != nil {
				t.logger.Error("[TimingQueue] execute expire operation, redis ZRangeWithScores", zap.Error(err))
				t.resetTimer(time.Second) // retry
			}

			if len(result) == 0 {
				t.resetTimer(maxDuration) // disable this select case gradually, waiting for new element add.
				goto next
			}

			t.heapTop = &Element{
				Value:     result[0].Member.(string),
				Timestamp: int64(result[0].Score),
			}
			t.resetTimer(time.Duration(t.heapTop.Timestamp-time.Now().Unix()) * time.Second)
		case <-exit:
			// stop product element from queue, exit queue
			close(t.nextC)
			t.timer.Stop()
			t.logger.Info("[TimingQueue] exit")
			return
		}
	}
}

// Push element to the min heap.
func (t *queue) Push(ctx context.Context, timestamp int64, value string) error {
	if err := t.backend.ZAdd(ctx, key, &redis.Z{
		Score:  float64(timestamp),
		Member: value,
	}).Err(); err != nil {
		return err
	}

	if t.heapTop == nil || t.heapTop.Timestamp > timestamp {
		t.heapTop = &Element{
			Value:     value,
			Timestamp: timestamp,
		}
		t.resetTimer(time.Duration(timestamp-time.Now().Unix()) * time.Second)
	}
	return nil
}

// BlockPop return queue element for consume.
func (t *queue) BlockPop() <-chan Element {
	return t.nextC
}

// Timestamp return the timestamp of this value
func (t *queue) Timestamp(ctx context.Context, value string) (int64, error) {
	resp, err := t.backend.ZScore(ctx, key, value).Result()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	return int64(resp), nil
}

// reset timer according to the min heap top value.
func (t *queue) resetTimer(expiration time.Duration) {
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
	t.timer.Reset(expiration)
}
