package bucket

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

var (
	task1 = "1"
	task2 = "2"
	task3 = "3"

	expiration4 = int64(1)

	expiration1 = int64(2)
	expiration2 = int64(3)
	expiration3 = int64(4)
)

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

	b := New(backend, Index(1, 1))
	backend.Del(context.Background(), b.Index())

	require.Nil(t, b.Add(context.Background(), task1))
	b.SetExpiration(expiration3)
	require.Equal(t, b.Expiration(), expiration3)
	require.Nil(t, b.Add(context.Background(), task2))
	require.Equal(t, b.Expiration(), expiration3)
	b.SetExpiration(expiration1)
	require.Equal(t, b.Expiration(), expiration1)
	require.Nil(t, b.Add(context.Background(), task3))
	b.SetExpiration(expiration2)
	require.Equal(t, b.Expiration(), expiration1)

	b.Flush(func(value string) {
		t.Log(value)
	})

	b = New(backend, b.Index(), WithExpiration(expiration4))
	require.Equal(t, b.Expiration(), expiration4)
}
