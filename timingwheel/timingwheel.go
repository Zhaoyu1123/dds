package timingwheel

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"dds/timingwheel/bucket"
	"dds/timingwheel/job"
	"dds/timingwheel/timingqueue"
	"dds/workerpool"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

const AlreadyExpired = AsyncTaskError("task already expired")

type AsyncTaskError string

func (e AsyncTaskError) Error() string { return string(e) }

// TimingWheel used for record delay job.
type TimingWheel struct {
	backend *redis.ClusterClient

	logger *zap.Logger

	// Trigger period, unit second
	tick int64
	// wheel size
	wheelSize int64
	// The level of the current wheel
	level int64

	// The period of time represented by the current wheel, unit second.
	interval int64
	// current timestamp
	currentTime int64

	queue timingqueue.TimingQueue

	buckets map[string]bucket.Bucket

	workers workerpool.WorkerPool

	overflowWheel unsafe.Pointer
}

// New creates an instance of TimingWheel.
func New(exit chan struct{}, wp workerpool.WorkerPool, backend *redis.ClusterClient, logger *zap.Logger,
	callback func(element timingqueue.Element)) (*TimingWheel, error) {
	timingQueue, err := timingqueue.New(exit, backend, logger)
	if err != nil {
		return nil, err
	}
	tw := newTimingWheel(1, 60, time.Now().Unix(), 0, timingQueue, wp, backend, logger)
	go tw.start(exit, callback)
	return tw, nil
}

// newTimingWheel is an internal helper function that really creates an instance of TimingWheel.
func newTimingWheel(tick, wheelSize, currentTime, level int64, queue timingqueue.TimingQueue,
	wp workerpool.WorkerPool, backend *redis.ClusterClient, logger *zap.Logger) *TimingWheel {
	return &TimingWheel{
		tick:        tick,
		wheelSize:   wheelSize,
		currentTime: currentTime,
		level:       level,
		interval:    tick * wheelSize,
		queue:       queue,
		workers:     wp,
		backend:     backend,
		buckets:     make(map[string]bucket.Bucket),
		logger:      logger,
	}
}

// run waiting for element expire of TimingQueue, then invoke callback logic and
// advance clock of current time wheel.
func (tw *TimingWheel) start(exit chan struct{}, callback func(element timingqueue.Element)) {
	tw.logger.Info("[TimingWheel] start, wait for bucket to expire")
	defer func() {
		if err := recover(); err != nil {
			tw.logger.Error("[TimingWheel] start panic", zap.Any("error", err))
		}
	}()

	for {
		select {
		case element := <-tw.queue.BlockPop():
			// NOTE: bucket Flush need to be invoked in callback.
			// if in distribute system, element need send to follower, follower will invoker bucket Flush.
			tw.workers.ScheduleAlways(func() {
				callback(element)
			})
			tw.advanceClock(element.Timestamp)
		case <-exit:
			tw.logger.Info("[TimingWheel] start exit")
			return
		}
	}
}

// Add a job to timing wheel.
func (tw *TimingWheel) Add(ctx context.Context, j job.Job) error {
	if err := j.SetState(ctx, job.Pending); err != nil {
		return fmt.Errorf("[TimingWheel] Add set `Pending` state error: %s", err.Error())
	}
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if j.NextExecuteTime.Unix() < currentTime+tw.tick {
		// Already expired
		if err := j.SetState(ctx, job.Failure, job.WithError(AlreadyExpired.Error())); err != nil {
			return fmt.Errorf("[TimingWheel] Add set `Failure` state error: %s", err.Error())
		}
		return fmt.Errorf("task already expired")
	} else if j.NextExecuteTime.Unix() < currentTime+tw.interval {
		// Put it into its own bucket
		index := (j.NextExecuteTime.Unix() / tw.tick) % tw.wheelSize

		bucketIndex := bucket.Index(tw.level, index)
		b, ok := tw.buckets[bucketIndex]
		if !ok {
			expiration, err := tw.queue.Timestamp(ctx, bucketIndex)
			if err != nil {
				return fmt.Errorf("[TimingWheel] Add timing queue `Timestamp` error: %s", err.Error())
			}
			b = bucket.New(tw.backend, bucketIndex, bucket.WithExpiration(expiration))
			tw.buckets[bucketIndex] = b
		}

		if err := b.Add(ctx, j.Id); err != nil {
			return fmt.Errorf("[TimingWheel] Add bucket Add error: %s", err.Error())
		}

		// Set the bucket expiration time
		if b.SetExpiration(j.NextExecuteTime.Unix()) {
			// The bucket needs to be enqueued since it was an expired bucket.
			// We only need to enqueue the bucket when its expiration time has changed,
			// i.e. the wheel has advanced and this bucket get reused with a new expiration.
			// Any further calls to set the expiration within the same wheel cycle will
			// pass in the same value and hence return false, thus the bucket with the
			// same expiration will not be enqueued multiple times.
			if err := tw.queue.Push(ctx, j.NextExecuteTime.Unix(), b.Index()); err != nil {
				return fmt.Errorf("[TimingWheel] Add timing queue `Push` error: %s", err.Error())
			}
		}
		if err := j.SetState(ctx, job.Received, job.WithPartition(b.Index())); err != nil {
			return fmt.Errorf("[TimingWheel] Add set `Received` state error: %s", err.Error())
		}
		return nil
	} else {
		// Out of the interval. Put it into the overflow wheel
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel == nil {
			atomic.CompareAndSwapPointer(
				&tw.overflowWheel,
				nil,
				unsafe.Pointer(newTimingWheel(
					tw.interval,
					tw.wheelSize,
					currentTime,
					tw.level+1,
					tw.queue,
					tw.workers,
					tw.backend,
					tw.logger,
				)),
			)
			overflowWheel = atomic.LoadPointer(&tw.overflowWheel)
		}
		return (*TimingWheel)(overflowWheel).Add(ctx, j)
	}
}

// Advance TimingWheel`s current timestamp.
func (tw *TimingWheel) advanceClock(expiration int64) {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if expiration >= currentTime+tw.tick {
		atomic.StoreInt64(&tw.currentTime, expiration)

		// Try to advance the clock of the overflow wheel if present
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel != nil {
			(*TimingWheel)(overflowWheel).advanceClock(currentTime)
		}
	}
}
