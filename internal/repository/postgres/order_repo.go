package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Patchanonss/flash-sale-engine/internal/domain"
	_ "github.com/lib/pq"
)

type orderRepository struct {
	db *sql.DB
}

// NewOrderRepository creates a new PostgreSQL-backed OrderRepository
func NewOrderRepository(db *sql.DB) domain.OrderRepository {
	return &orderRepository{db: db}
}

func (r *orderRepository) Create(ctx context.Context, order *domain.Order) error {
	query := `
		INSERT INTO orders (id, user_id, item_id, quantity, total_price, status, idempotency_key, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var idempotencyVal sql.NullString
	if order.IdempotencyKey != "" {
		idempotencyVal = sql.NullString{String: order.IdempotencyKey, Valid: true}
	}

	_, err := r.db.ExecContext(
		ctx,
		query,
		order.ID,
		order.UserID,
		order.ItemID,
		order.Quantity,
		order.TotalPrice,
		string(order.Status),
		idempotencyVal,
		order.CreatedAt,
		order.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert order: %w", err)
	}

	return nil
}

func (r *orderRepository) GetByID(ctx context.Context, id string) (*domain.Order, error) {
	query := `
		SELECT id, user_id, item_id, quantity, total_price, status, idempotency_key, created_at, updated_at
		FROM orders
		WHERE id = $1
	`
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var order domain.Order
	var statusStr string
	var idempotencyVal sql.NullString

	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&order.ID,
		&order.UserID,
		&order.ItemID,
		&order.Quantity,
		&order.TotalPrice,
		&statusStr,
		&idempotencyVal,
		&order.CreatedAt,
		&order.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrOrderNotFound
		}
		return nil, fmt.Errorf("failed to query order by id: %w", err)
	}

	order.Status = domain.OrderStatus(statusStr)
	if idempotencyVal.Valid {
		order.IdempotencyKey = idempotencyVal.String
	}

	return &order, nil
}

func (r *orderRepository) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Order, error) {
	query := `
		SELECT id, user_id, item_id, quantity, total_price, status, idempotency_key, created_at, updated_at
		FROM orders
		WHERE idempotency_key = $1
	`
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var order domain.Order
	var statusStr string
	var idempotencyVal sql.NullString

	err := r.db.QueryRowContext(ctx, query, key).Scan(
		&order.ID,
		&order.UserID,
		&order.ItemID,
		&order.Quantity,
		&order.TotalPrice,
		&statusStr,
		&idempotencyVal,
		&order.CreatedAt,
		&order.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // Not found is not an error for idempotency check
		}
		return nil, fmt.Errorf("failed to query order by idempotency key: %w", err)
	}

	order.Status = domain.OrderStatus(statusStr)
	if idempotencyVal.Valid {
		order.IdempotencyKey = idempotencyVal.String
	}

	return &order, nil
}

func (r *orderRepository) ListRecent(ctx context.Context, limit int) ([]*domain.Order, error) {
	if limit <= 0 {
		limit = 20
	}

	query := `
		SELECT id, user_id, item_id, quantity, total_price, status, idempotency_key, created_at, updated_at
		FROM orders
		ORDER BY created_at DESC
		LIMIT $1
	`
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list recent orders: %w", err)
	}
	defer rows.Close()

	var orders []*domain.Order
	for rows.Next() {
		var o domain.Order
		var statusStr string
		var idempotencyVal sql.NullString

		if err := rows.Scan(
			&o.ID,
			&o.UserID,
			&o.ItemID,
			&o.Quantity,
			&o.TotalPrice,
			&statusStr,
			&idempotencyVal,
			&o.CreatedAt,
			&o.UpdatedAt,
		); err != nil {
			return nil, err
		}

		o.Status = domain.OrderStatus(statusStr)
		if idempotencyVal.Valid {
			o.IdempotencyKey = idempotencyVal.String
		}
		orders = append(orders, &o)
	}

	return orders, nil
}
