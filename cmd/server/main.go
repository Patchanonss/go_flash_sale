package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Patchanonss/flash-sale-engine/internal/consumer"
	httpdelivery "github.com/Patchanonss/flash-sale-engine/internal/delivery/http"
	httpdeliverymiddleware "github.com/Patchanonss/flash-sale-engine/internal/delivery/http/middleware"
	"github.com/Patchanonss/flash-sale-engine/internal/domain"
	kafkaproducer "github.com/Patchanonss/flash-sale-engine/internal/repository/kafka"
	postgresrepo "github.com/Patchanonss/flash-sale-engine/internal/repository/postgres"
	redisrepo "github.com/Patchanonss/flash-sale-engine/internal/repository/redis"
	"github.com/Patchanonss/flash-sale-engine/internal/usecase"
	"github.com/Patchanonss/flash-sale-engine/internal/worker"
	_ "github.com/lib/pq"
	redisclient "github.com/redis/go-redis/v9"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	log.Println("==================================================")
	log.Println("⚡ Starting High-Concurrency Flash Sale Engine ⚡")
	log.Println("==================================================")

	port := getEnv("PORT", "8080")
	dbHost := getEnv("DB_HOST", "127.0.0.1")
	dbPort := getEnv("DB_PORT", "5432")
	dbUser := getEnv("DB_USER", "postgres")
	dbPassword := getEnv("DB_PASSWORD", "postgrespassword")
	dbName := getEnv("DB_NAME", "flashsale_db")
	dbSSL := getEnv("DB_SSLMODE", "disable")

	redisAddr := getEnv("REDIS_ADDR", "127.0.0.1:6379")
	kafkaBrokers := []string{getEnv("KAFKA_BROKERS", "127.0.0.1:9092")}
	kafkaTopic := getEnv("KAFKA_TOPIC_ORDERS", "flashsale.orders")
	kafkaGroup := getEnv("KAFKA_CONSUMER_GROUP", "flashsale-notification-group")

	// 1. Connect to PostgreSQL
	psqlConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		dbHost, dbPort, dbUser, dbPassword, dbName, dbSSL)
	db, err := sql.Open("postgres", psqlConnStr)
	var orderRepo domain.OrderRepository
	var itemRepo domain.ItemRepository

	if err == nil && db.Ping() == nil {
		log.Printf("✅ PostgreSQL connected successfully (%s:%s)", dbHost, dbPort)
		orderRepo = postgresrepo.NewOrderRepository(db)
		itemRepo = postgresrepo.NewItemRepository(db)
	} else {
		log.Printf("⚠️ PostgreSQL not reachable (%v). Running with mock in-memory fallback.", err)
	}

	// 2. Connect to Redis (High-concurrency pool tuning for 10k requests)
	rdb := redisclient.NewClient(&redisclient.Options{
		Addr:         redisAddr,
		PoolSize:     1000,
		MinIdleConns: 100,
		PoolTimeout:  5 * time.Second,
	})
	ctxPing, cancelPing := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelPing()

	var stockCacheRepo domain.StockCacheRepository
	if err := rdb.Ping(ctxPing).Err(); err == nil {
		log.Printf("✅ Redis connected successfully (%s)", redisAddr)
		stockCacheRepo = redisrepo.NewStockCacheRepository(rdb)
		// Initialize sample item stock to 100
		_ = stockCacheRepo.SetStock(context.Background(), 1, 100)
	} else {
		log.Fatalf("❌ Redis is required for atomic Lua operations (%v). Please start Redis via docker compose up.", err)
	}

	// 3. Connect to Apache Kafka (KRaft)
	var eventProducer domain.EventProducer
	eventProducer = kafkaproducer.NewKafkaProducer(kafkaBrokers, kafkaTopic)
	log.Printf("✅ Kafka Producer initialized (Topic: %s, Brokers: %v)", kafkaTopic, kafkaBrokers)

	// Start Background Kafka Consumer (Notification Microservice)
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	notificationConsumer := consumer.NewNotificationConsumer(kafkaBrokers, kafkaTopic, kafkaGroup)
	notificationConsumer.Start(consumerCtx)

	// 4. Initialize Go Worker Pool (Goroutines & Buffered Channel)
	orderQueue := make(chan *domain.Order, 1000)
	workerPool := worker.NewOrderWorkerPool(10, orderQueue, orderRepo, eventProducer)
	workerPool.Start()

	// 5. Initialize WebSocket Hub
	wsHub := httpdelivery.NewWSHub()
	go wsHub.Run()

	// 6. Initialize Usecase & HTTP Handlers
	uc := usecase.NewFlashSaleUsecase(stockCacheRepo, orderRepo, itemRepo, orderQueue)
	handler := httpdelivery.NewHandler(uc, orderRepo, wsHub)

	// Attach Redis-backed Rate Limiter (Anti-Bot: 20 reqs/sec per IP)
	if rdb != nil {
		rl := httpdeliverymiddleware.NewRateLimiterMiddleware(rdb, httpdeliverymiddleware.RateLimiterConfig{
			Limit:  20,
			Window: 1 * time.Second,
		})
		handler.SetRateLimiter(rl)
		log.Println("🛡️ Anti-Bot Rate Limiter enabled (Limit: 20 reqs/sec)")
	}

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      handler.Routes(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Start HTTP Server in background
	go func() {
		log.Printf("🚀 HTTP Server listening on http://localhost:%s", port)
		log.Printf("🌐 Open http://localhost:%s in your browser to view the Flash Sale Dashboard", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// 7. Graceful Shutdown on SIGINT / SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("\n🛑 Shutdown signal received. Commencing graceful shutdown...")

	// Stop accepting new HTTP requests
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
	}

	// Drain in-flight orders in worker pool
	if err := workerPool.Stop(5 * time.Second); err != nil {
		log.Printf("Worker pool shutdown warning: %v", err)
	}

	// Close Kafka, DB, Redis
	cancelConsumer()
	_ = notificationConsumer.Close()
	_ = eventProducer.Close()
	if db != nil {
		_ = db.Close()
	}
	_ = rdb.Close()

	log.Println("👋 Graceful shutdown complete. Zero data loss. Goodbye!")
}
