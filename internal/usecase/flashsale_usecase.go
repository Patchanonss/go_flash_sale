package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/Patchanonss/flash-sale-engine/internal/domain"
	"github.com/google/uuid"
)

type FlashSaleUsecase interface {
	PlaceOrder(ctx context.Context, req *domain.CreateOrderRequest) (*domain.Order, int, error)
	GetItemStock(ctx context.Context, itemID int64) (int, error)
	ResetStock(ctx context.Context, itemID int64, stock int) error
}

type flashSaleUsecase struct {
	stockCacheRepo domain.StockCacheRepository
	orderRepo      domain.OrderRepository
	itemRepo       domain.ItemRepository
	orderQueue     chan<- *domain.Order // Channel to asynchronous worker pool
}

func NewFlashSaleUsecase(
	stockCacheRepo domain.StockCacheRepository,
	orderRepo domain.OrderRepository,
	itemRepo domain.ItemRepository,
	orderQueue chan<- *domain.Order,
) FlashSaleUsecase {
	return &flashSaleUsecase{
		stockCacheRepo: stockCacheRepo,
		orderRepo:      orderRepo,
		itemRepo:       itemRepo,
		orderQueue:     orderQueue,
	}
}

func (u *flashSaleUsecase) PlaceOrder(ctx context.Context, req *domain.CreateOrderRequest) (*domain.Order, int, error) {
	if req.Quantity <= 0 {
		return nil, 0, domain.ErrInvalidQuantity
	}

	// 1. Check idempotency in Redis (microsecond lock to prevent concurrent double-clicks)
	if req.IdempotencyKey != "" {
		acquired, err := u.stockCacheRepo.SetIdempotencyNX(ctx, req.IdempotencyKey, 24*time.Hour)
		if err == nil && !acquired {
			return nil, 0, domain.ErrDuplicateOrder
		}
		if u.orderRepo != nil {
			existingOrder, err := u.orderRepo.GetByIdempotencyKey(ctx, req.IdempotencyKey)
			if err == nil && existingOrder != nil {
				return existingOrder, 0, nil
			}
		}
	}

	// 2. Atomic Stock Deduction in Redis via Lua Script (<1ms)
	success, remainingStock, err := u.stockCacheRepo.DeductStockAtomic(ctx, req.ItemID, req.Quantity)
	if err != nil {
		return nil, 0, err
	}
	if !success {
		return nil, remainingStock, domain.ErrOutOfStock
	}

	// 3. Build Order Object
	orderID := uuid.New().String()
	order := &domain.Order{
		ID:             orderID,
		UserID:         req.UserID,
		ItemID:         req.ItemID,
		Quantity:       req.Quantity,
		TotalPrice:     float64(req.Quantity) * 499.00, // Default price or fetch from itemRepo
		Status:         domain.OrderStatusPending,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	// 4. Asynchronous Handoff: Push order to worker pool channel (non-blocking if buffered)
	if u.orderQueue != nil {
		select {
		case u.orderQueue <- order:
			// Enqueued successfully
		default:
			// If buffer is completely saturated, could log or fallback
			return nil, remainingStock, fmt.Errorf("order queue is currently saturated")
		}
	}

	return order, remainingStock, nil
}

func (u *flashSaleUsecase) GetItemStock(ctx context.Context, itemID int64) (int, error) {
	return u.stockCacheRepo.GetStock(ctx, itemID)
}

func (u *flashSaleUsecase) ResetStock(ctx context.Context, itemID int64, stock int) error {
	if stock < 0 {
		return domain.ErrInvalidQuantity
	}
	// Update in Redis
	if err := u.stockCacheRepo.SetStock(ctx, itemID, stock); err != nil {
		return err
	}
	// Update in PostgreSQL if itemRepo is available
	if u.itemRepo != nil {
		_ = u.itemRepo.ResetStock(ctx, itemID, stock)
	}
	return nil
}
