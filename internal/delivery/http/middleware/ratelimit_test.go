package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redisclient "github.com/redis/go-redis/v9"
)

func TestRateLimiterMiddleware(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redisclient.NewClient(&redisclient.Options{
		Addr: mr.Addr(),
	})

	// Configure 5 requests per 2 seconds
	cfg := RateLimiterConfig{
		Limit:  5,
		Window: 2 * time.Second,
	}

	mw := NewRateLimiterMiddleware(rdb, cfg)

	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	chained := mw(dummyHandler)

	// Send 5 requests -> all should succeed
	for i := 1; i <= 5; i++ {
		req := httptest.NewRequest("GET", "/api/orders", nil)
		req.RemoteAddr = "192.168.1.50:12345"
		w := httptest.NewRecorder()

		chained.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d expected status 200 OK, got %d", i, w.Code)
		}
	}

	// 6th request -> should be throttled (429)
	req := httptest.NewRequest("GET", "/api/orders", nil)
	req.RemoteAddr = "192.168.1.50:12345"
	w := httptest.NewRecorder()

	chained.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected status 429 Too Many Requests, got %d", w.Code)
	}

	if w.Header().Get("Retry-After") == "" {
		t.Fatalf("expected Retry-After header to be present")
	}
}
