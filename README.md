# High-Concurrency Flash Sale & Event-Driven Order Engine

A high-throughput, low-latency Flash Sale order engine implemented in **Go (Golang)** designed to solve the classic inventory overselling problem under extreme concurrency.

---

## 🚀 Key Highlights & Architecture

* **Sub-Millisecond Atomic Stock Deduction:** Employs atomic **Redis Lua Scripts** to evaluate and decrement inventory in-memory, completely preventing race conditions and database overselling.
* **Asynchronous Order Persistence (Worker Pool):** Buffered Go channels and a pool of concurrent Goroutine workers decouple fast checkout responses (`202 Accepted`) from relational database writes.
* **Resilient Database Writes (Exponential Backoff):** Handles transient database write contention on **PostgreSQL** with automatic exponential backoff retries and dead-letter queue (DLQ) protection.
* **Event-Driven Decoupling via Apache Kafka (KRaft):** Dispatches domain events (`OrderPlacedEvent`) to **Kafka**, enabling independent downstream consumers (e.g., Notifications, Real-time Sales Analytics) with offset-based replayability.
* **Anti-Bot Rate Limiting:** Distributed **Token-Bucket Rate Limiter** middleware prevents flash sale abuse per user/IP.
* **Real-time Stock Synchronization:** Pushes instant inventory updates to connected web clients via **WebSockets**.
* **Clean Architecture & Automated CI:** Strict separation of domain, use cases, repositories, and delivery handlers, with race detection verified via **GitHub Actions** (`go test -race`).

---

## 🛠️ Tech Stack

* **Language:** Go 1.22+
* **Cache & Locks:** Redis 7 (Lua Scripting, Distributed Token Bucket)
* **Message Streaming:** Apache Kafka 3.7 (KRaft mode)
* **Primary Database:** PostgreSQL 16 (ACID, Indexes)
* **API & Protocols:** REST API, WebSocket
* **Frontend:** Next.js, Tailwind CSS (Deployable to Vercel)
* **Infra & DevOps:** Docker Compose, GitHub Actions CI

---

## 🏗️ System Architecture Flow

```
[ Client / Web UI ]
       │  (POST /api/orders)
       ▼
[ Rate Limiter Middleware (Redis) ]
       │
       ▼
[ Atomic Stock Check & Decr (Redis Lua) ] ── (Out of Stock) ──> 400 Bad Request
       │
       ├─ (Stock Available) ──> 202 Accepted (Order Queued)
       ▼
[ Go Worker Pool (Goroutines & Channels) ]
       │
       ├─> PostgreSQL (Batch / Transactional Insert with Exponential Backoff)
       ├─> WebSocket Hub (Broadcast remaining stock)
       └─> Apache Kafka Topic: `flashsale.orders`
                 │
                 ├─> [ Consumer 1: Notification Service ]
                 └─> [ Consumer 2: Analytics Service ]
```

---

## 🚦 Quick Start (Local Setup)

### 1. Start Infrastructure (Postgres, Redis, Kafka)
```bash
docker compose up -d
```

### 2. Verify Health
```bash
docker compose ps
```

### 3. Run the Go Server
```bash
go run cmd/server/main.go
```
