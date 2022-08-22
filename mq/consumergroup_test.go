package mq

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

func TestTopics(t *testing.T) {
	t.Log(os.Getenv("KAFKA_USERNAME"), os.Getenv("KAFKA_PASSWORD"))
	mechanism, err := scram.Mechanism(scram.SHA256, os.Getenv("KAFKA_USERNAME"), os.Getenv("KAFKA_PASSWORD"))
	if err != nil {
		t.Fatal(err)
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
	}

	conn, err := dialer.DialContext(context.Background(), "tcp", strings.Split(os.Getenv("KAFKA_BROKERS"), ",")[0])
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	m := map[string]struct{}{}

	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	for k := range m {
		t.Log(k)
	}
}

func TestConsumerGroup(t *testing.T) {
	exit := make(chan struct{})
	go InitConsumerGroup(exit, strings.Split(os.Getenv("KAFKA_BROKERS"), ","), []string{os.Getenv("KAFKA_TOPIC")},
		os.Getenv("KAFKA_GROUP_ID"), zap.NewExample(), func(value []byte) error {
			t.Log("test log message", value)
			return nil
		}, WithAuthenticate(os.Getenv("KAFKA_USERNAME"), os.Getenv("KAFKA_PASSWORD")))
	time.Sleep(20 * time.Second)
	close(exit)
	t.Log("exit")
	time.Sleep(20 * time.Second)
}
