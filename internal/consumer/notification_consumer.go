package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"sync"
	"time"

	"github.com/Patchanonss/flash-sale-engine/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

type NotificationConsumer struct {
	reader *kafkago.Reader
	wg     sync.WaitGroup
}

// NewNotificationConsumer initializes a Kafka Consumer for order confirmation notifications
func NewNotificationConsumer(brokers []string, topic string, groupID string) *NotificationConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 1 * time.Second,
		StartOffset:    kafkago.FirstOffset,
	})

	return &NotificationConsumer{
		reader: reader,
	}
}

// Start runs the message consumption loop in a background goroutine
func (c *NotificationConsumer) Start(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		log.Printf("[NotificationConsumer] Listening for Kafka events on topic '%s' (Group: '%s')...",
			c.reader.Config().Topic, c.reader.Config().GroupID)

		for {
			select {
			case <-ctx.Done():
				log.Printf("[NotificationConsumer] Shutting down consumer loop...")
				return
			default:
				// Fetch message with context
				msg, err := c.reader.FetchMessage(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
						return
					}
					log.Printf("[NotificationConsumer] Error fetching message: %v", err)
					time.Sleep(500 * time.Millisecond)
					continue
				}

				// Process event
				var event domain.OrderPlacedEvent
				if err := json.Unmarshal(msg.Value, &event); err != nil {
					log.Printf("[NotificationConsumer] Failed to unmarshal message: %v", err)
					_ = c.reader.CommitMessages(ctx, msg)
					continue
				}

				// Business logic: simulate email notification
				log.Printf("[NotificationService] 📩 Sent Order Confirmation Email to User: %s | OrderID: %s | Total: $%.2f",
					event.UserID, event.OrderID, event.TotalPrice)

				// Commit offset to guarantee at-least-once processing
				if err := c.reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("[NotificationConsumer] Failed to commit message offset: %v", err)
				}
			}
		}
	}()
}

// Close gracefully closes the Kafka reader connection
func (c *NotificationConsumer) Close() error {
	c.wg.Wait()
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
