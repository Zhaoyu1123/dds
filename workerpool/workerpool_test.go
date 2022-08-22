package workerpool

import (
	"runtime"
	"testing"
	"time"

	"go.uber.org/zap"
)

func BenchmarkShardPool_ScheduleAuto(b *testing.B) {
	sp := NewShardPool(1, runtime.GOMAXPROCS(0), zap.NewExample())
	for i := 0; i < b.N; i++ {
		sp.ScheduleAuto(func() {})
	}
	sp.Close()
}

func TestSchedule(t *testing.T) {
	size := 5
	pool := NewPool(size, zap.NewExample())
	p := pool.(*Pool)
	for i := 0; i < 5; i++ {
		pool.Schedule(func() {
			time.Sleep(100 * time.Millisecond)
		})
	}
	now := time.Now()
	pool.Schedule(func() {})
	if time.Now().Before(now.Add(50 * time.Millisecond)) {
		t.Errorf("Test Schedule() error, should wait for 20 millisecond")
	}
	if len(p.worker) != size {
		t.Errorf("Test Schedule() error, should be 5")
	}
}

func TestScheduleAlways(t *testing.T) {
	size := 5
	pool := NewPool(size, zap.NewExample())
	p := pool.(*Pool)
	for i := 0; i < 20; i++ {
		pool.ScheduleAlways(func() {
		})
		time.Sleep(10 * time.Millisecond)
	}

	if len(p.worker) == 1 {
		t.Errorf("Test ScheduleAlways() error, should not be 1")
	}

	for i := 0; i < 5; i++ {
		pool.ScheduleAlways(func() {
			time.Sleep(100 * time.Millisecond)
		})
	}
	now := time.Now()
	pool.ScheduleAlways(func() {})
	if time.Now().After(now.Add(50 * time.Millisecond)) {
		t.Errorf("Test Schedule() error, should run it now")
	}
	if len(p.worker) != size {
		t.Errorf("Test Schedule() error, should be 5")
	}
}

func TestScheduleAuto(t *testing.T) {
	size := 5
	pool := NewPool(size, zap.NewExample())
	p := pool.(*Pool)
	for i := 0; i < 3; i++ {
		pool.ScheduleAuto(func() {
			time.Sleep(time.Millisecond)
		})
		time.Sleep(50 * time.Millisecond)
	}
	if len(p.worker) != 1 {
		t.Errorf("Test ScheduleAuto() error, should be 1, but get %d", len(p.worker))
	}

	for i := 0; i < 3; i++ {
		pool.ScheduleAuto(func() {
			time.Sleep(10 * time.Millisecond)
		})
	}
	time.Sleep(50 * time.Millisecond)
	if len(p.worker) != 3 {
		t.Errorf("Test ScheduleAuto() error, should be 3, but get %d", len(p.worker))
	}

	for i := 0; i < 10; i++ {
		pool.ScheduleAuto(func() {
			time.Sleep(time.Millisecond)
		})
	}
	time.Sleep(10 * time.Millisecond)
	if len(p.worker) != size {
		t.Errorf("Test ScheduleAuto() error, should be %d, but get %d", size, len(p.worker))
	}

	p.Close()
}

func TestPanic(t *testing.T) {
	pool := NewPool(10, zap.NewExample())
	p := pool.(*Pool)
	pool.ScheduleAlways(func() {
		panic("hello")
	})
	time.Sleep(10 * time.Millisecond)
	if len(p.worker) != 0 {
		t.Errorf("Test ScheduleAuto() error, should be 0")
	}
}
