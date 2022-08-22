package dds

import (
	"context"
	"dds/mq"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"dds/node"
	"dds/timingwheel/job"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
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
	job.SetBackend(backend)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{os.Getenv("ETCD_ENDPOINTS")},
		DialTimeout: 5 * time.Second,
	})
	require.Nil(t, err)

	exit := make(chan struct{})
	logger := zap.NewExample()
	currentNode, err := node.NewNode(exit, logger, client, ":9090")
	require.Nil(t, err)

	dds, err := New(exit, logger, backend, currentNode, mq.Config{
		Brokers:  strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		Topic:    os.Getenv("KAFKA_TOPIC"),
		GroupId:  os.Getenv("KAFKA_GROUP_ID"),
		Username: os.Getenv("KAFKA_USERNAME"),
		Password: os.Getenv("KAFKA_PASSWORD"),
	})
	require.Nil(t, err)

	time.Sleep(5 * time.Second)
	spec := fmt.Sprint(time.Now().Add(time.Second * 5).Unix())

	id, err := dds.Schedule(context.Background(), spec, []byte("hello"))
	require.Nil(t, err)
	t.Log(id)
	time.Sleep(10 * time.Second)
}
