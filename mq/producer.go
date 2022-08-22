package mq

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

func NewProducer(cfg Config, l *zap.Logger) (Producer, error) {
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

	return &producer{writer: kafka.NewWriter(kafka.WriterConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,

		Dialer:       dialer,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: int(kafka.RequireAll),
		Async:        true,
		Logger:       infoLogger{l},
		ErrorLogger:  errorLogger{l},
	})}, nil

}

type producer struct {
	writer *kafka.Writer
}

func (p *producer) Product(ctx context.Context, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Value: value,
	})
}
