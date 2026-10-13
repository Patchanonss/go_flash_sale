package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	BaseURL        string
	TotalUsers     int           // Total distinct users attempting to buy
	Concurrency    int           // Concurrent worker goroutines
	InitialStock   int           // Inventory for the flash sale
	HumanJitterMax time.Duration // Random reflex delay (0 to N ms)
	DoubleClickPct float64       // % of users who accidentally double-click (idempotency test)
}

type Result struct {
	StatusCode int
	Latency    time.Duration
	Err        error
}

func main() {
	cfg := Config{
		BaseURL:        "http://127.0.0.1:8080",
		TotalUsers:     60000,
		Concurrency:    1500,
		InitialStock:   3000,
		HumanJitterMax: 5 * time.Millisecond,
		DoubleClickPct: 0.10, // 5% accidental double clicks
	}

	fmt.Println("=================================================================")
	fmt.Println("🚀 Realistic Flash Sale Load Test Simulation")
	fmt.Println("=================================================================")
	fmt.Printf("• Target URL:          %s\n", cfg.BaseURL)
	fmt.Printf("• Initial Stock:       %d units\n", cfg.InitialStock)
	fmt.Printf("• Total Unique Users:  %d buyers\n", cfg.TotalUsers)
	fmt.Printf("• Concurrency Level:   %d concurrent goroutines\n", cfg.Concurrency)
	fmt.Printf("• Human Reflex Jitter: 0 - %v\n", cfg.HumanJitterMax)
	fmt.Printf("• Accidental Clicks:   %.1f%% double-clicks\n", cfg.DoubleClickPct*100)
	fmt.Println("-----------------------------------------------------------------")

	// 1. Reset Inventory to InitialStock before beginning
	fmt.Println("🔄 Resetting stock to initial state...")
	if err := resetStock(cfg.BaseURL, cfg.InitialStock); err != nil {
		log.Fatalf("❌ Failed to reset stock: %v (Is the Go server running on %s?)", err, cfg.BaseURL)
	}
	time.Sleep(500 * time.Millisecond)

	// HTTP Client with optimized connection pooling
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Concurrency * 2,
			MaxIdleConnsPerHost: cfg.Concurrency * 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	// Channel of user IDs
	userChan := make(chan int, cfg.TotalUsers)
	for i := 1; i <= cfg.TotalUsers; i++ {
		userChan <- i
	}
	close(userChan)

	resultsChan := make(chan Result, cfg.TotalUsers*2)
	var wg sync.WaitGroup

	var (
		successCount  int64
		soldOutCount  int64
		throttleCount int64
		errorCount    int64
	)

	fmt.Println("⚡ Flash Sale Countdown hit 00:00! Firing traffic storm...")
	startTime := time.Now()

	// Launch worker pool
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for userID := range userChan {
				// Simulate human reflex jitter (staggered clicks within 0-50ms)
				jitter := time.Duration(rand.Int63n(int64(cfg.HumanJitterMax)))
				time.Sleep(jitter)

				// Unique user characteristics
				uID := fmt.Sprintf("buyer-%04d", userID)
				clientIP := fmt.Sprintf("192.168.%d.%d", (userID/250)+1, (userID%250)+1)
				idempKey := fmt.Sprintf("idemp-%s-%d", uID, time.Now().UnixNano())

				// Perform Primary Order Request
				res := executeOrder(client, cfg.BaseURL, uID, clientIP, idempKey)
				resultsChan <- res

				switch res.StatusCode {
				case http.StatusAccepted:
					atomic.AddInt64(&successCount, 1)
				case http.StatusConflict:
					atomic.AddInt64(&soldOutCount, 1)
				case http.StatusTooManyRequests:
					atomic.AddInt64(&throttleCount, 1)
				default:
					atomic.AddInt64(&errorCount, 1)
				}

				// Simulate accidental double-click (same user, same idempotency key)
				if rand.Float64() < cfg.DoubleClickPct {
					resDup := executeOrder(client, cfg.BaseURL, uID, clientIP, idempKey)
					resultsChan <- resDup
				}
			}
		}(w)
	}

	wg.Wait()
	close(resultsChan)
	elapsed := time.Since(startTime)

	// Collect latencies for statistical analysis
	var latencies []time.Duration
	for res := range resultsChan {
		if res.Err == nil {
			latencies = append(latencies, res.Latency)
		}
	}

	// 3. Verify Final Inventory & Overselling
	finalStock, _ := fetchCurrentStock(cfg.BaseURL)

	// 4. Output Statistical Report
	printReport(elapsed, len(latencies), successCount, soldOutCount, throttleCount, errorCount, latencies, cfg.InitialStock, finalStock)
}

func executeOrder(client *http.Client, baseURL, userID, clientIP, idempKey string) Result {
	reqBody, _ := json.Marshal(map[string]interface{}{
		"user_id":         userID,
		"item_id":         1,
		"quantity":        1,
		"idempotency_key": idempKey,
	})

	req, err := http.NewRequest("POST", baseURL+"/api/orders", bytes.NewReader(reqBody))
	if err != nil {
		return Result{Err: err}
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forwarded-For", clientIP)
	req.Header.Set("Idempotency-Key", idempKey)

	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start)

	if err != nil {
		return Result{Err: err, Latency: latency}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return Result{
		StatusCode: resp.StatusCode,
		Latency:    latency,
	}
}

func resetStock(baseURL string, stock int) error {
	payload, _ := json.Marshal(map[string]int{"stock": stock})
	resp, err := http.Post(baseURL+"/api/items/1/reset", "application/json", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func fetchCurrentStock(baseURL string) (int, error) {
	resp, err := http.Get(baseURL+"/api/items/1/stock")
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()

	var data struct {
		Stock int `json:"stock"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&data)
	return data.Stock, nil
}

func printReport(elapsed time.Duration, totalReq int, success, soldOut, throttle, errors int64, latencies []time.Duration, initialStock, finalStock int) {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	var totalLatency time.Duration
	for _, l := range latencies {
		totalLatency += l
	}

	avgLatency := time.Duration(0)
	p50 := time.Duration(0)
	p95 := time.Duration(0)
	p99 := time.Duration(0)
	minLat := time.Duration(0)
	maxLat := time.Duration(0)

	n := len(latencies)
	if n > 0 {
		avgLatency = totalLatency / time.Duration(n)
		minLat = latencies[0]
		maxLat = latencies[n-1]
		p50 = latencies[int(float64(n)*0.50)]
		p95 = latencies[int(float64(n)*0.95)]
		p99 = latencies[int(float64(n)*0.99)]
	}

	rps := float64(totalReq) / elapsed.Seconds()

	fmt.Println("\n=================================================================")
	fmt.Println("📊 LOAD TEST BENCHMARK RESULTS")
	fmt.Println("=================================================================")
	fmt.Printf("• Total Duration:         %.2fs\n", elapsed.Seconds())
	fmt.Printf("• Total Requests Executed: %d\n", totalReq)
	fmt.Printf("• Throughput (RPS):       %.1f requests/sec\n", rps)
	fmt.Println("-----------------------------------------------------------------")
	fmt.Println("📈 LATENCY DISTRIBUTION:")
	fmt.Printf("  - Min:                  %v\n", minLat)
	fmt.Printf("  - Average:              %v\n", avgLatency)
	fmt.Printf("  - p50 (Median):         %v\n", p50)
	fmt.Printf("  - p95:                  %v\n", p95)
	fmt.Printf("  - p99:                  %v\n", p99)
	fmt.Printf("  - Max:                  %v\n", maxLat)
	fmt.Println("-----------------------------------------------------------------")
	fmt.Println("📦 INVENTORY & CONSISTENCY INTEGRITY:")
	fmt.Printf("  - Initial Stock:        %d\n", initialStock)
	fmt.Printf("  - Confirmed Orders:     %d (202 Accepted)\n", success)
	fmt.Printf("  - Sold Out Rejections:  %d (409 Conflict)\n", soldOut)
	fmt.Printf("  - Rate-Limited Drops:   %d (429 Too Many Requests)\n", throttle)
	fmt.Printf("  - Network/Other Errors: %d\n", errors)
	fmt.Printf("  - Final Stock in Redis: %d\n", finalStock)

	oversold := success - int64(initialStock)
	if oversold < 0 {
		oversold = 0
	}

	fmt.Println("-----------------------------------------------------------------")
	if oversold == 0 && finalStock == 0 && success == int64(initialStock) {
		fmt.Printf("🏆 VERDICT: PERFECT CONSISTENCY! 0 Oversold Units.\n")
		fmt.Printf("   All %d items allocated atomically. %d users rejected safely.\n", initialStock, soldOut)
	} else {
		fmt.Printf("⚠️ VERDICT: Inconsistency detected! Oversold: %d\n", oversold)
	}
	fmt.Println("=================================================================")
}
