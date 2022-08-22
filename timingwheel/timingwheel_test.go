package timingwheel

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"dds/timingwheel/bucket"
	"dds/timingwheel/job"
	"dds/timingwheel/timingqueue"
	"dds/workerpool"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	m.Run()
}

func TestExample(t *testing.T) {
	o := &redis.ClusterOptions{
		Addrs:              []string{os.Getenv("REDIS_ENDPOINTS")},
		NewClient:          nil,
		MaxRedirects:       0,
		ReadOnly:           false,
		RouteByLatency:     false,
		RouteRandomly:      false,
		ClusterSlots:       nil,
		Dialer:             nil,
		OnConnect:          nil,
		MaxRetries:         0,
		MinRetryBackoff:    0,
		MaxRetryBackoff:    0,
		DialTimeout:        10 * time.Second,
		ReadTimeout:        3 * time.Second,
		WriteTimeout:       3 * time.Second,
		PoolFIFO:           false,
		PoolSize:           10 * runtime.GOMAXPROCS(0),
		MinIdleConns:       10,
		MaxConnAge:         0,
		PoolTimeout:        4 * time.Second,
		IdleTimeout:        5 * time.Minute,
		IdleCheckFrequency: 60 * time.Second,
		TLSConfig:          nil,
	}
	backend := redis.NewClusterClient(o)
	job.SetBackend(backend)

	callback := func(element timingqueue.Element) {
		b := bucket.New(backend, element.Value)
		b.Flush(func(value string) {
			t.Log(value)
		})
	}

	logger := zap.NewExample()
	workers := workerpool.NewShardPool(256, runtime.NumCPU(), logger)
	exit := make(chan struct{})
	tw, err := New(exit, workers, backend, logger, callback)
	require.Equal(t, nil, err)
	require.Equal(t, nil, tw.Add(context.Background(), job.Job{
		Id:              "1",
		NextExecuteTime: time.Now().Add(2 * time.Second),
		ExecuteTimeSpec: "",
		RetryCount:      0,
		RetryBackoff:    0,
		Data:            nil,
		State:           "",
	}))

	require.Equal(t, nil, tw.Add(context.Background(), job.Job{
		Id:              "2",
		NextExecuteTime: time.Now().Add(3 * time.Second),
		ExecuteTimeSpec: "",
		RetryCount:      0,
		RetryBackoff:    0,
		Data:            nil,
		State:           "",
	}))

	require.Equal(t, nil, tw.Add(context.Background(), job.Job{
		Id:              "3",
		NextExecuteTime: time.Now().Add(5 * time.Second),
		ExecuteTimeSpec: "",
		RetryCount:      0,
		RetryBackoff:    0,
		Data:            nil,
		State:           "",
	}))

	time.Sleep(20 * time.Second)
}
