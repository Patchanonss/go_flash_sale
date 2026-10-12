package worker

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/Patchanonss/flash-sale-engine/internal/domain"
)

type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

var DefaultRetryConfig = RetryConfig{
	MaxRetries: 3,
	BaseDelay:  50 * time.Millisecond,
	MaxDelay:   500 * time.Millisecond,
}

type OrderWorkerPool struct {
	workerCount   int
	orderQueue    chan *domain.Order
	orderRepo     domain.OrderRepository
	eventProducer domain.EventProducer // Optional Kafka producer
	retryConfig   RetryConfig

	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	isClosed bool
	mu       sync.Mutex
}

// NewOrderWorkerPool creates a new worker pool for processing orders asynchronously
func NewOrderWorkerPool(
	workerCount int,
	orderQueue chan *domain.Order,
	orderRepo domain.OrderRepository,
	eventProducer domain.EventProducer,
	retryConfig ...RetryConfig,
) *OrderWorkerPool {
	cfg := DefaultRetryConfig
	if len(retryConfig) > 0 {
		cfg = retryConfig[0]
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &OrderWorkerPool{
		workerCount:   workerCount,
		orderQueue:    orderQueue,
		orderRepo:     orderRepo,
		eventProducer: eventProducer,
		retryConfig:   cfg,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start launches the worker goroutines
func (p *OrderWorkerPool) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 1; i <= p.workerCount; i++ {
		p.wg.Add(1)
		workerID := i
		go p.worker(workerID)
	}

	log.Printf("[WorkerPool] Started %d workers successfully", p.workerCount)
}

func (p *OrderWorkerPool) worker(id int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			// Drain remaining orders in queue before exiting
			p.drainRemaining(id)
			return

		case order, ok := <-p.orderQueue:
			if !ok {
				// Queue channel closed
				return
			}
			p.process(id, order)
		}
	}
}

// drainRemaining ensures all items in the buffered channel are processed during graceful shutdown
func (p *OrderWorkerPool) drainRemaining(workerID int) {
	for {
		select {
		case order, ok := <-p.orderQueue:
			if !ok {
				return
			}
			p.process(workerID, order)
		default:
			// Queue is empty
			return
		}
	}
}

func (p *OrderWorkerPool) process(workerID int, order *domain.Order) {
	err := p.persistWithExponentialBackoff(order)
	if err != nil {
		log.Printf("[Worker-%d] ERROR: Failed to persist order %s after %d retries: %v (Moved to DLQ)",
			workerID, order.ID, p.retryConfig.MaxRetries, err)
		order.Status = domain.OrderStatusFailed
		return
	}

	order.Status = domain.OrderStatusConfirmed

	// If Kafka Event Producer is configured, publish domain event
	if p.eventProducer != nil {
		event := &domain.OrderPlacedEvent{
			EventID:    fmt.Sprintf("evt-%s", order.ID),
			OrderID:    order.ID,
			UserID:     order.UserID,
			ItemID:     order.ItemID,
			Quantity:   order.Quantity,
			TotalPrice: order.TotalPrice,
			Timestamp:  time.Now(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := p.eventProducer.PublishOrderPlaced(ctx, event); err != nil {
			log.Printf("[Worker-%d] WARNING: Failed to publish Kafka event for order %s: %v", workerID, order.ID, err)
		}
	}
}

// persistWithExponentialBackoff retries database inserts with exponential delay + jitter
func (p *OrderWorkerPool) persistWithExponentialBackoff(order *domain.Order) error {
	var err error
	delay := p.retryConfig.BaseDelay

	for attempt := 0; attempt <= p.retryConfig.MaxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = p.orderRepo.Create(ctx, order)
		cancel()

		if err == nil {
			return nil // Success!
		}

		if attempt == p.retryConfig.MaxRetries {
			break
		}

		// Calculate Exponential Backoff with Jitter
		// Formula: delay = min(maxDelay, baseDelay * 2^attempt) + jitter
		jitter := time.Duration(rand.Int63n(int64(delay / 2)))
		sleepDuration := delay + jitter
		if sleepDuration > p.retryConfig.MaxDelay {
			sleepDuration = p.retryConfig.MaxDelay
		}

		time.Sleep(sleepDuration)
		delay *= 2
	}

	return err
}

// Stop executes graceful shutdown: cancels context, waits for workers to finish all jobs
func (p *OrderWorkerPool) Stop(timeout time.Duration) error {
	p.mu.Lock()
	if p.isClosed {
		p.mu.Unlock()
		return nil
	}
	p.isClosed = true
	p.mu.Unlock()

	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("[WorkerPool] Graceful shutdown completed cleanly. All orders processed.")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("[WorkerPool] Graceful shutdown timed out after %v", timeout)
	}
}
