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

	// 1. Unified Atomic Execution in Redis (<1ms):
	// Performs Idempotency validation + Stock deduction in a single Lua operation.
	// Eliminates redundant round-trips and removes PostgreSQL query from the hot path completely.
	success, remainingStock, err := u.stockCacheRepo.DeductStockAtomic(ctx, req.ItemID, req.Quantity, req.IdempotencyKey, 24*time.Hour)
	if err != nil {
		if err == domain.ErrDuplicateOrder {
			return nil, 0, domain.ErrDuplicateOrder
		}
		if err == domain.ErrOutOfStock {
			return nil, remainingStock, domain.ErrOutOfStock
		}
		return nil, 0, err
	}
	if !success {
		return nil, remainingStock, domain.ErrOutOfStock
	}

	// 2. Build Order Object
	orderID := uuid.New().String()
	order := &domain.Order{
		ID:             orderID,
		UserID:         req.UserID,
		ItemID:         req.ItemID,
		Quantity:       req.Quantity,
		TotalPrice:     float64(req.Quantity) * 499.00,
		Status:         domain.OrderStatusPending,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	// 3. Asynchronous Handoff: Push order to worker pool channel (non-blocking if buffered)
	if u.orderQueue != nil {
		select {
		case u.orderQueue <- order:
			// Enqueued successfully
		default:
			// Buffer saturated! Roll back Redis stock and idempotency key to prevent phantom inventory loss
			_ = u.stockCacheRepo.RollbackStock(ctx, req.ItemID, req.Quantity, req.IdempotencyKey)
			return nil, remainingStock + req.Quantity, fmt.Errorf("order queue is currently saturated")
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
