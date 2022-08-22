package mq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

type consumer struct {
	group   *kafka.ConsumerGroup
	logger  *zap.Logger
	dialer  *kafka.Dialer
	brokers []string
}

func NewConsumer(cfg Config, l *zap.Logger) (Consumer, error) {
	dialer := &kafka.Dialer{
		DualStack: true,
	}

	if cfg.Username != "" && cfg.Password != "" {
		mechanism, err := scram.Mechanism(scram.SHA256, cfg.Username, cfg.Password)
		if err != nil {
			return nil, err
		}

		dialer.SASLMechanism = mechanism
	}

	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		Dialer:      dialer,
		ID:          cfg.GroupId,
		Brokers:     cfg.Brokers,
		Topics:      []string{cfg.Topic},
		Logger:      infoLogger{l},
		ErrorLogger: errorLogger{l},
	})
	if err != nil {
		return nil, err
	}

	return &consumer{
		brokers: cfg.Brokers,
		dialer:  dialer,
		group:   group,
		logger:  l,
	}, nil
}

func (c *consumer) wait(exit chan struct{}) {
	select {
	case <-exit:
		c.group.Close()
	}
}

func (c *consumer) Consume(exit chan struct{}, f func(value []byte) error) {
	go c.wait(exit)
	for {
		gen, err := c.group.Next(context.TODO())
		if err != nil {
			if errors.Is(err, kafka.ErrGroupClosed) {
				c.logger.Info("consumer group exit")
				return
			}
			c.logger.Error("consumer group Next", zap.Error(err))
			return
		}

		for topic := range gen.Assignments {
			partitionAssignments := gen.Assignments[topic]
			for _, assignment := range partitionAssignments {
				partition, offset := assignment.ID, assignment.Offset
				gen.Start(func(ctx context.Context) {
					c.logger.Info(fmt.Sprintf("consumer start reader at topic[%s], partition[%d], offset[%d]", topic, partition, offset))
					// create reader for this partition.
					reader := kafka.NewReader(kafka.ReaderConfig{
						Dialer:         c.dialer,
						Brokers:        c.brokers,
						Topic:          topic,
						Partition:      partition,
						CommitInterval: time.Second,
					})
					defer reader.Close()

					if err := reader.SetOffset(offset); err != nil {
						c.logger.Error("consumer reader SetOffset", zap.Error(err))
					}
					for {
						msg, err := reader.ReadMessage(ctx)
						if err != nil {
							if errors.Is(err, kafka.ErrGenerationEnded) {
								c.logger.Info("consumer reader exit")
								return
							}
							c.logger.Error("consumer reader ReadMessage", zap.Error(err))
						}

						c.logger.Info(fmt.Sprintf("received message topic[%s], partition[%d], offset[%d], key[%s], value[%s], time[%s]",
							msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value), msg.Time.String()))

						if err := f(msg.Value); err != nil {
							c.logger.Error("consumer callback invoke", zap.Error(err))
						}

						if err := gen.CommitOffsets(map[string]map[int]int64{msg.Topic: {msg.Partition: msg.Offset + 1}}); err != nil {
							c.logger.Error("consumer generation CommitOffsets", zap.Error(err))
						}
					}
				})
			}
		}
	}
}

// InitConsumerGroup only use for test case.
func InitConsumerGroup(exit chan struct{}, brokers []string, topics []string, groupId string, l *zap.Logger,
	callback func(value []byte) error, funcOptions ...FuncOptions) error {
	op := &options{}
	for _, f := range funcOptions {
		f(op)
	}

	dialer := &kafka.Dialer{
		DualStack: true,
	}

	if op.username != "" && op.password != "" {
		mechanism, err := scram.Mechanism(scram.SHA256, op.username, op.password)
		if err != nil {
			return err
		}

		dialer.SASLMechanism = mechanism
	}

	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		Dialer:      dialer,
		ID:          groupId,
		Brokers:     brokers,
		Topics:      topics,
		Logger:      infoLogger{l},
		ErrorLogger: errorLogger{l},
	})
	if err != nil {
		l.Error("xxxxxx")
		return err
	}

	go func() {
		select {
		case <-exit:
			group.Close()
		}
	}()

	for {
		gen, err := group.Next(context.TODO())
		if err != nil {
			if errors.Is(err, kafka.ErrGroupClosed) {
				return err
			}
			l.Error("err", zap.Error(err))
			return err
		}

		for topic := range gen.Assignments {
			partitionAssignments := gen.Assignments[topic]
			for _, assignment := range partitionAssignments {
				partition, offset := assignment.ID, assignment.Offset
				gen.Start(func(ctx context.Context) {
					l.Info(fmt.Sprintf("start reader at topic[%s], partition[%d], offset[%d]", topic, partition, offset))
					// create reader for this partition.
					reader := kafka.NewReader(kafka.ReaderConfig{
						Dialer:         dialer,
						Brokers:        brokers,
						Topic:          topic,
						Partition:      partition,
						CommitInterval: time.Second,
					})
					defer reader.Close()

					if err := reader.SetOffset(offset); err != nil {
						l.Error("reader SetOffset", zap.Error(err))
					}
					for {
						msg, err := reader.ReadMessage(ctx)
						if err != nil {
							if errors.Is(err, kafka.ErrGenerationEnded) {
								l.Info("message", zap.Any("msg", msg))
								return
							}
							l.Error("reader ReadMessage", zap.Error(err))
						}

						l.Info(fmt.Sprintf("received message topic[%s], partition[%d], offset[%d], key[%s], value[%s], time[%s]",
							msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value), msg.Time.String()))

						if err := callback(msg.Value); err != nil {
							l.Error("callback invoke", zap.Error(err))
						}

						if err := gen.CommitOffsets(map[string]map[int]int64{msg.Topic: {msg.Partition: msg.Offset + 1}}); err != nil {
							l.Error("generation CommitOffsets", zap.Error(err))
						}
					}
				})
			}
		}
	}
}
