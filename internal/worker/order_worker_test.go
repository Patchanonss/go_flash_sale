package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Patchanonss/flash-sale-engine/internal/domain"
)

// mockOrderRepo implements domain.OrderRepository in-memory for testing
type mockOrderRepo struct {
	mu           sync.Mutex
	orders       map[string]*domain.Order
	failAttempts int32
	attemptsMap  map[string]int
}

func newMockOrderRepo() *mockOrderRepo {
	return &mockOrderRepo{
		orders:      make(map[string]*domain.Order),
		attemptsMap: make(map[string]int),
	}
}

func (m *mockOrderRepo) Create(ctx context.Context, order *domain.Order) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.attemptsMap[order.ID]++

	// If failAttempts is configured, fail until the threshold is passed
	if atomic.LoadInt32(&m.failAttempts) > 0 {
		if m.attemptsMap[order.ID] <= int(atomic.LoadInt32(&m.failAttempts)) {
			return errors.New("simulated transient postgres deadlock")
		}
	}

	m.orders[order.ID] = order
	return nil
}

func (m *mockOrderRepo) GetByID(ctx context.Context, id string) (*domain.Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.orders[id]
	if !ok {
		return nil, domain.ErrOrderNotFound
	}
	return o, nil
}

func (m *mockOrderRepo) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, o := range m.orders {
		if o.IdempotencyKey == key {
			return o, nil
		}
	}
	return nil, nil
}

func (m *mockOrderRepo) ListRecent(ctx context.Context, limit int) ([]*domain.Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var list []*domain.Order
	for _, o := range m.orders {
		list = append(list, o)
	}
	return list, nil
}

func TestOrderWorkerPool_ConcurrentProcessing(t *testing.T) {
	queue := make(chan *domain.Order, 100)
	repo := newMockOrderRepo()

	pool := NewOrderWorkerPool(5, queue, repo, nil)
	pool.Start()

	totalOrders := 50
	for i := 1; i <= totalOrders; i++ {
		queue <- &domain.Order{
			ID:         fmt.Sprintf("order-%d", i),
			UserID:     fmt.Sprintf("user-%d", i),
			ItemID:     1,
			Quantity:   1,
			TotalPrice: 499.00,
			Status:     domain.OrderStatusPending,
			CreatedAt:  time.Now(),
		}
	}

	// Stop gracefully with timeout
	err := pool.Stop(2 * time.Second)
	if err != nil {
		t.Fatalf("expected graceful shutdown, got error: %v", err)
	}

	repo.mu.Lock()
	savedCount := len(repo.orders)
	repo.mu.Unlock()

	if savedCount != totalOrders {
		t.Fatalf("expected %d orders saved in database, got %d", totalOrders, savedCount)
	}
}

func TestOrderWorkerPool_ExponentialBackoffRetry(t *testing.T) {
	queue := make(chan *domain.Order, 10)
	repo := newMockOrderRepo()

	// Simulate 2 transient failures, succeed on 3rd attempt
	atomic.StoreInt32(&repo.failAttempts, 2)

	retryCfg := RetryConfig{
		MaxRetries: 3,
		BaseDelay:  10 * time.Millisecond,
		MaxDelay:   50 * time.Millisecond,
	}

	pool := NewOrderWorkerPool(1, queue, repo, nil, retryCfg)
	pool.Start()

	testOrder := &domain.Order{
		ID:         "order-retry-test",
		UserID:     "user-1",
		ItemID:     1,
		Quantity:   1,
		TotalPrice: 499.00,
		Status:     domain.OrderStatusPending,
		CreatedAt:  time.Now(),
	}

	queue <- testOrder

	err := pool.Stop(2 * time.Second)
	if err != nil {
		t.Fatalf("expected clean stop, got: %v", err)
	}

	repo.mu.Lock()
	attempts := repo.attemptsMap[testOrder.ID]
	_, saved := repo.orders[testOrder.ID]
	repo.mu.Unlock()

	if attempts != 3 {
		t.Fatalf("expected exactly 3 attempts (2 retries + 1 success), got %d", attempts)
	}

	if !saved {
		t.Fatalf("order should have been saved after retries")
	}

	if testOrder.Status != domain.OrderStatusConfirmed {
		t.Fatalf("order status should be CONFIRMED, got %s", testOrder.Status)
	}
}
