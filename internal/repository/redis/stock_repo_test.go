package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Patchanonss/flash-sale-engine/internal/domain"
	"github.com/alicebob/miniredis/v2"
	redisclient "github.com/redis/go-redis/v9"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, domain.StockCacheRepository) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redisclient.NewClient(&redisclient.Options{
		Addr: mr.Addr(),
	})

	repo := NewStockCacheRepository(client)
	return mr, repo
}

func TestStockCacheRepository_BasicOperations(t *testing.T) {
	mr, repo := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	itemID := int64(100)

	// Test Get on nonexistent item
	_, err := repo.GetStock(ctx, itemID)
	if err != domain.ErrItemNotFound {
		t.Fatalf("expected ErrItemNotFound, got %v", err)
	}

	// Test SetStock
	err = repo.SetStock(ctx, itemID, 50)
	if err != nil {
		t.Fatalf("failed to set stock: %v", err)
	}

	// Test GetStock
	stock, err := repo.GetStock(ctx, itemID)
	if err != nil {
		t.Fatalf("failed to get stock: %v", err)
	}
	if stock != 50 {
		t.Fatalf("expected stock 50, got %d", stock)
	}
}

func TestStockCacheRepository_DeductStockAtomic(t *testing.T) {
	mr, repo := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	itemID := int64(200)

	// 1. Set initial stock to 10
	err := repo.SetStock(ctx, itemID, 10)
	if err != nil {
		t.Fatalf("failed to set stock: %v", err)
	}

	// 2. Deduct 4 items -> remaining should be 6
	success, remaining, err := repo.DeductStockAtomic(ctx, itemID, 4, "", 0)
	if err != nil || !success {
		t.Fatalf("deduct failed: success=%v, err=%v", success, err)
	}
	if remaining != 6 {
		t.Fatalf("expected remaining stock 6, got %d", remaining)
	}

	// 3. Deduct 6 items -> remaining should be 0
	success, remaining, err = repo.DeductStockAtomic(ctx, itemID, 6, "", 0)
	if err != nil || !success {
		t.Fatalf("deduct failed: success=%v, err=%v", success, err)
	}
	if remaining != 0 {
		t.Fatalf("expected remaining stock 0, got %d", remaining)
	}

	// 4. Try to deduct 1 item when stock is 0 -> should return ErrOutOfStock and success=false
	success, remaining, err = repo.DeductStockAtomic(ctx, itemID, 1, "", 0)
	if err != domain.ErrOutOfStock {
		t.Fatalf("expected ErrOutOfStock, got %v", err)
	}
	if success {
		t.Fatalf("expected success=false, got true")
	}
	if remaining != 0 {
		t.Fatalf("expected remaining stock 0, got %d", remaining)
	}
}

func TestStockCacheRepository_UnifiedIdempotency(t *testing.T) {
	mr, repo := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	itemID := int64(400)
	err := repo.SetStock(ctx, itemID, 5)
	if err != nil {
		t.Fatalf("failed to set stock: %v", err)
	}

	idempKey := "test-idemp-123"

	// 1st request with idempotency key -> success
	success, remaining, err := repo.DeductStockAtomic(ctx, itemID, 1, idempKey, 1*time.Hour)
	if err != nil || !success || remaining != 4 {
		t.Fatalf("expected success, got success=%v, remaining=%d, err=%v", success, remaining, err)
	}

	// 2nd request with SAME idempotency key -> ErrDuplicateOrder without decrementing stock!
	success, remaining, err = repo.DeductStockAtomic(ctx, itemID, 1, idempKey, 1*time.Hour)
	if err != domain.ErrDuplicateOrder {
		t.Fatalf("expected ErrDuplicateOrder, got err=%v", err)
	}
	if success {
		t.Fatalf("expected success=false for duplicate request")
	}

	// Verify stock is still 4
	stock, _ := repo.GetStock(ctx, itemID)
	if stock != 4 {
		t.Fatalf("stock was decremented by duplicate request! expected 4, got %d", stock)
	}
}

func TestStockCacheRepository_RollbackStock(t *testing.T) {
	mr, repo := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	itemID := int64(500)
	_ = repo.SetStock(ctx, itemID, 5)

	idempKey := "test-rollback-key"

	// Deduct 1 item
	_, remaining, err := repo.DeductStockAtomic(ctx, itemID, 1, idempKey, 1*time.Hour)
	if err != nil || remaining != 4 {
		t.Fatalf("deduct failed: %v", err)
	}

	// Rollback 1 item
	err = repo.RollbackStock(ctx, itemID, 1, idempKey)
	if err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	// Verify stock restored to 5
	stock, _ := repo.GetStock(ctx, itemID)
	if stock != 5 {
		t.Fatalf("expected stock restored to 5, got %d", stock)
	}

	// Verify idempotency key deleted: can use the same key again
	success, remaining, err := repo.DeductStockAtomic(ctx, itemID, 1, idempKey, 1*time.Hour)
	if err != nil || !success || remaining != 4 {
		t.Fatalf("expected to reuse key after rollback, got %v", err)
	}
}

func TestStockCacheRepository_ConcurrentDeductions(t *testing.T) {
	mr, repo := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	itemID := int64(300)
	initialStock := 50
	totalRequests := 100 // 100 concurrent requests trying to buy 1 item each

	err := repo.SetStock(ctx, itemID, initialStock)
	if err != nil {
		t.Fatalf("failed to set stock: %v", err)
	}

	var wg sync.WaitGroup
	var successCount int64
	var failCount int64
	var mu sync.Mutex

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			success, _, err := repo.DeductStockAtomic(ctx, itemID, 1, "", 0)
			mu.Lock()
			if success && err == nil {
				successCount++
			} else {
				failCount++
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	if successCount != int64(initialStock) {
		t.Fatalf("expected exactly %d successful orders, but got %d", initialStock, successCount)
	}

	if failCount != int64(totalRequests-initialStock) {
		t.Fatalf("expected %d failed orders, but got %d", totalRequests-initialStock, failCount)
	}

	finalStock, err := repo.GetStock(ctx, itemID)
	if err != nil {
		t.Fatalf("failed to get final stock: %v", err)
	}
	if finalStock != 0 {
		t.Fatalf("expected final stock to be exactly 0 (no overselling), got %d", finalStock)
	}
}
