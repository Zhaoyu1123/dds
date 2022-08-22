package timingqueue

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

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
	exit := make(chan struct{})
	q, err := New(exit, backend, zap.NewExample())
	require.Equal(t, nil, err)

	m := map[string]int64{
		"a": time.Now().Add(2 * time.Second).Unix(),
		"b": time.Now().Add(3 * time.Second).Unix(),
		"c": time.Now().Add(4 * time.Second).Unix(),
	}
	go func() {
		for element := range q.BlockPop() {
			fmt.Println(time.Now().Unix(), element.Timestamp)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.Equal(t, nil, q.Push(ctx, m["a"], "a"))
	require.Equal(t, nil, q.Push(ctx, m["b"], "b"))
	require.Equal(t, nil, q.Push(ctx, m["c"], "c"))
	val, err := q.Timestamp(ctx, "c")

	require.Equal(t, nil, err)
	fmt.Println(val, "c")
	time.Sleep(7 * time.Second)
}
