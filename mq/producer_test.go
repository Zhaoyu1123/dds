package mq

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestProduct(t *testing.T) {
	p, err := NewProducer(Config{
		Brokers:  strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		Topic:    os.Getenv("KAFKA_TOPIC"),
		GroupId:  os.Getenv("KAFKA_GROUP_ID"),
		Username: os.Getenv("KAFKA_USERNAME"),
		Password: os.Getenv("KAFKA_PASSWORD"),
	}, zap.NewExample())

	err = p.Product(context.Background(), []byte("world"))
	require.Nil(t, nil, err)
	time.Sleep(10 * time.Second)
}
