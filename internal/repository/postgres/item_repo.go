package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Patchanonss/flash-sale-engine/internal/domain"
)

type itemRepository struct {
	db *sql.DB
}

// NewItemRepository creates a new PostgreSQL-backed ItemRepository
func NewItemRepository(db *sql.DB) domain.ItemRepository {
	return &itemRepository{db: db}
}

func (r *itemRepository) GetByID(ctx context.Context, id int64) (*domain.Item, error) {
	query := `
		SELECT id, name, total_stock, price, created_at, updated_at
		FROM items
		WHERE id = $1
	`
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var item domain.Item
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&item.ID,
		&item.Name,
		&item.TotalStock,
		&item.Price,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrItemNotFound
		}
		return nil, fmt.Errorf("failed to query item: %w", err)
	}

	return &item, nil
}

func (r *itemRepository) ResetStock(ctx context.Context, itemID int64, stock int) error {
	query := `
		UPDATE items
		SET total_stock = $1, updated_at = NOW()
		WHERE id = $2
	`
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, err := r.db.ExecContext(ctx, query, stock, itemID)
	if err != nil {
		return fmt.Errorf("failed to reset item stock: %w", err)
	}

	return nil
}
