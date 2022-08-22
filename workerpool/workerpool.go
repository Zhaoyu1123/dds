package workerpool

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// ShardPool divide a large Pool into multiple smaller Pool.
type ShardPool struct {
	shards         []WorkerPool
	numberOfShards int64
}

// NewShardPool create a new ShardPool.
func NewShardPool(size, numberOfShards int, logger *zap.Logger) WorkerPool {
	shards := make([]WorkerPool, numberOfShards)
	for idx := range shards {
		shards[idx] = NewPool(size/numberOfShards, logger)
	}
	return &ShardPool{
		shards:         shards,
		numberOfShards: int64(numberOfShards),
	}
}

// shard randomly assign a Pool.
func (s *ShardPool) shard() WorkerPool {
	randomVal := rand.New(rand.NewSource(time.Now().UnixNano())).Int63()
	return s.shards[randomVal%s.numberOfShards]
}

// Schedule like Pool.Schedule, See Pool.Schedule
// documentation for more information.
func (s *ShardPool) Schedule(task func()) {
	shard := s.shard()
	shard.Schedule(task)
}

// ScheduleAlways like Pool.ScheduleAlways, See Pool.ScheduleAlways
// documentation for more information.
func (s *ShardPool) ScheduleAlways(task func()) {
	shard := s.shard()
	shard.ScheduleAlways(task)
}

// ScheduleAuto like Pool.ScheduleAuto, See Pool.ScheduleAuto
// documentation for more information.
func (s *ShardPool) ScheduleAuto(task func()) {
	shard := s.shard()
	shard.ScheduleAuto(task)
}

func (s *ShardPool) Close() {
	for _, p := range s.shards {
		p.Close()
	}
}

// Pool defined goroutine pool, Worker will receive and execute task from job channel.
type Pool struct {
	sync.RWMutex

	wg sync.WaitGroup

	job    chan func()
	worker chan struct{}

	// use for monitor pool status.
	uuid        string
	size        int
	currentSize int

	logger *zap.Logger
}

// NewPool create a new Pool with n workers.
func NewPool(n int, logger *zap.Logger) WorkerPool {
	return &Pool{
		uuid:   uuid.New().String(),
		job:    make(chan func()),
		worker: make(chan struct{}, n),
		size:   n,
		logger: logger,
	}
}

// Schedule acquire a goroutine to execute task from Pool.
func (p *Pool) Schedule(task func()) {
	select {
	case p.job <- task:
	case p.worker <- struct{}{}:
		// create a goroutine to do job.
		p.add(1)
		go p.spawnWorker(task)
	}
}

// ScheduleAlways acquire a goroutine to execute task from Pool.
// If Pool is overflow, Acquire a new temp goroutine for task.
func (p *Pool) ScheduleAlways(task func()) {
	select {
	case p.job <- task:
	case p.worker <- struct{}{}:
		p.add(1)
		// create a goroutine to do job.
		go p.spawnWorker(task)
	default:
		// create a temp goroutine to do job.
		p.add(1)
		go p.spawnWorkerWithTimeout(task, time.Minute)
	}
}

// ScheduleAuto executes Schedule logic first.
// If Schedule failed, then executes ScheduleAlways logic.
func (p *Pool) ScheduleAuto(task func()) {
	select {
	case p.job <- task:
		return
	default:
	}
	select {
	case p.job <- task:
	case p.worker <- struct{}{}:
		// create a goroutine to do job.
		p.add(1)
		go p.spawnWorker(task)
	default:
		// create a temp goroutine to do job.
		p.add(1)
		go p.spawnWorkerWithTimeout(task, time.Minute)
	}
}

// spawnWorker should never exit, always try to acquire task from job channel.
func (p *Pool) spawnWorker(task func()) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error(
				fmt.Sprintf("Pool[%s] size/current[%d/%d] worker exit with panic.", p.uuid, p.size, p.current()),
				zap.Any("Panic", r))
		}
		<-p.worker
		p.done()
	}()

	task()
	for job := range p.job {
		job()
	}
}

// spawnWorkerWithTimeout execute task and waiting for
// timeout. If don`t receive task in timeout then return, else do task and reset timer.
func (p *Pool) spawnWorkerWithTimeout(task func(), timeout time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error(
				fmt.Sprintf("Pool[%s] size/current[%d/%d] Temp worker exit with panic.", p.uuid, p.size, p.current()),
				zap.Any("Panic", r))
		}
		p.done()
	}()

	task()
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			timer.Stop()
			return
		case job, ok := <-p.job:
			if !ok {
				return
			}
			job()

			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
		}
	}
}

// Close the job channel and waiting for all task finish.
func (p *Pool) Close() {
	close(p.job)
	p.wg.Wait()
	p.logger.Info(fmt.Sprintf("Pool[%s] size/current[%d/%d] close.", p.uuid, p.size, p.current()))
}

func (p *Pool) add(n int) {
	p.wg.Add(n)
	p.Lock()
	p.currentSize += n
	p.Unlock()
	p.logger.Info(fmt.Sprintf("Pool[%s] size/current[%d/%d] create a worker for task execution.",
		p.uuid, p.size, p.current()))
}

func (p *Pool) done() {
	p.wg.Done()
	p.Lock()
	p.currentSize -= 1
	p.Unlock()
	p.logger.Info(fmt.Sprintf("Pool[%s] size/current[%d/%d] worker exit.", p.uuid, p.size, p.current()))
}

func (p *Pool) current() int {
	p.RLock()
	current := p.currentSize
	p.RUnlock()
	return current
}

// goWithRecover wraps a `go func()` with recover().
func goWithRecover(handler func(), recoverHandler func(r interface{})) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				if recoverHandler != nil {
					go func() {
						defer func() {
							if p := recover(); p != nil {
								fmt.Println(p, r)
							}
						}()
						recoverHandler(r)
					}()
				}
			}
		}()
		handler()
	}()
}
