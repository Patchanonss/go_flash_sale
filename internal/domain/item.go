package domain

import (
	"context"
	"time"
)

type Item struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	TotalStock int       `json:"total_stock"`
	Price      float64   `json:"price"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// ItemRepository defines data access methods for Items in PostgreSQL & Redis
type ItemRepository interface {
	GetByID(ctx context.Context, id int64) (*Item, error)
	ResetStock(ctx context.Context, itemID int64, stock int) error
}

// StockCacheRepository defines fast in-memory Redis operations for inventory
type StockCacheRepository interface {
	SetStock(ctx context.Context, itemID int64, stock int) error
	GetStock(ctx context.Context, itemID int64) (int, error)
	DeductStockAtomic(ctx context.Context, itemID int64, quantity int, idempotencyKey string, idempTTL time.Duration) (bool, int, error)
	RollbackStock(ctx context.Context, itemID int64, quantity int, idempotencyKey string) error
	SetIdempotencyNX(ctx context.Context, key string, ttl time.Duration) (bool, error)
}
