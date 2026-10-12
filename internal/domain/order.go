package domain

import (
	"context"
	"time"
)

type OrderStatus string

const (
	OrderStatusPending   OrderStatus = "PENDING"
	OrderStatusConfirmed OrderStatus = "CONFIRMED"
	OrderStatusFailed    OrderStatus = "FAILED"
)

type Order struct {
	ID             string      `json:"id"`
	UserID         string      `json:"user_id"`
	ItemID         int64       `json:"item_id"`
	Quantity       int         `json:"quantity"`
	TotalPrice     float64     `json:"total_price"`
	Status         OrderStatus `json:"status"`
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
}

type CreateOrderRequest struct {
	UserID         string `json:"user_id"`
	ItemID         int64  `json:"item_id"`
	Quantity       int    `json:"quantity"`
	IdempotencyKey string `json:"idempotency_key"`
}

type OrderRepository interface {
	Create(ctx context.Context, order *Order) error
	GetByID(ctx context.Context, id string) (*Order, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*Order, error)
	ListRecent(ctx context.Context, limit int) ([]*Order, error)
}
