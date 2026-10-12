package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Patchanonss/flash-sale-engine/internal/domain"
	redisclient "github.com/redis/go-redis/v9"
)

type stockCacheRepo struct {
	client *redisclient.Client
}

// NewStockCacheRepository creates a new Redis-backed StockCacheRepository
func NewStockCacheRepository(client *redisclient.Client) domain.StockCacheRepository {
	return &stockCacheRepo{client: client}
}

func stockKey(itemID int64) string {
	return fmt.Sprintf("item:stock:%d", itemID)
}

func (r *stockCacheRepo) SetStock(ctx context.Context, itemID int64, stock int) error {
	return r.client.Set(ctx, stockKey(itemID), stock, 0).Err()
}

func (r *stockCacheRepo) GetStock(ctx context.Context, itemID int64) (int, error) {
	val, err := r.client.Get(ctx, stockKey(itemID)).Result()
	if err != nil {
		if err == redisclient.Nil {
			return 0, domain.ErrItemNotFound
		}
		return 0, err
	}
	stock, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}
	return stock, nil
}

// DeductStockAtomic executes the Lua script atomically in Redis.
// Returns: (success, remainingStock, error)
func (r *stockCacheRepo) DeductStockAtomic(ctx context.Context, itemID int64, quantity int) (bool, int, error) {
	if quantity <= 0 {
		return false, 0, domain.ErrInvalidQuantity
	}

	keys := []string{stockKey(itemID)}
	args := []interface{}{quantity}

	res, err := r.client.Eval(ctx, DeductStockLuaScript, keys, args...).Result()
	if err != nil {
		return false, 0, fmt.Errorf("failed to evaluate lua script: %w", err)
	}

	// Lua returns an array: {status, remaining_stock}
	results, ok := res.([]interface{})
	if !ok || len(results) < 2 {
		return false, 0, fmt.Errorf("unexpected lua response format: %v", res)
	}

	status, ok := results[0].(int64)
	if !ok {
		return false, 0, fmt.Errorf("invalid status type returned by lua")
	}

	remainingStock, ok := results[1].(int64)
	if !ok {
		return false, 0, fmt.Errorf("invalid remaining stock type returned by lua")
	}

	switch status {
	case 1:
		// Deducted successfully
		return true, int(remainingStock), nil
	case 0:
		// Out of stock
		return false, int(remainingStock), domain.ErrOutOfStock
	case -1:
		// Stock key doesn't exist
		return false, 0, domain.ErrItemNotFound
	default:
		return false, 0, fmt.Errorf("unknown lua status code: %d", status)
	}
}

func (r *stockCacheRepo) SetIdempotencyNX(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return r.client.SetNX(ctx, "idemp:"+key, "1", ttl).Result()
}

