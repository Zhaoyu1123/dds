package job

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestState(t *testing.T) {

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

	cli := redis.NewClusterClient(o)

	SetBackend(cli)

	s := Job{
		Id:              "123123",
		NextExecuteTime: time.Now(),
		ExecuteTimeSpec: "",
		RetryCount:      0,
		RetryBackoff:    0,
		Data:            nil,
		State:           "",
	}
	s.SetState(context.Background(), Pending)
	require.Equal(t, s.State, Pending)
	s.SetState(context.Background(), Started)
	require.Equal(t, s.State, Started)

	a := time.Time{}
	t.Log(a.String())
	a.Unix()
}
