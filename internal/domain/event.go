package domain

import (
	"context"
	"time"
)

// OrderPlacedEvent is published to Apache Kafka once an order is confirmed
type OrderPlacedEvent struct {
	EventID    string    `json:"event_id"`
	OrderID    string    `json:"order_id"`
	UserID     string    `json:"user_id"`
	ItemID     int64     `json:"item_id"`
	Quantity   int       `json:"quantity"`
	TotalPrice float64   `json:"total_price"`
	Timestamp  time.Time `json:"timestamp"`
}

// EventProducer abstracts publishing events to messaging brokers (e.g., Kafka)
type EventProducer interface {
	PublishOrderPlaced(ctx context.Context, event *OrderPlacedEvent) error
	Close() error
}
