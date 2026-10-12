package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Patchanonss/flash-sale-engine/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

type kafkaProducer struct {
	writer *kafkago.Writer
}

// NewKafkaProducer creates a new EventProducer targeting Apache Kafka
func NewKafkaProducer(brokers []string, topic string) domain.EventProducer {
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafkago.RequireOne,
		Async:        false,
	}

	return &kafkaProducer{
		writer: writer,
	}
}

func (p *kafkaProducer) PublishOrderPlaced(ctx context.Context, event *domain.OrderPlacedEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal OrderPlacedEvent: %w", err)
	}

	msg := kafkago.Message{
		Key:   []byte(event.OrderID),
		Value: payload,
		Time:  event.Timestamp,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}

func (p *kafkaProducer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
