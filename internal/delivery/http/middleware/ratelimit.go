package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

// RateLimiterConfig defines parameters for request throttling
type RateLimiterConfig struct {
	Limit  int           // Maximum allowed requests in window
	Window time.Duration // Time window (e.g., 1 second)
}

var DefaultRateLimiterConfig = RateLimiterConfig{
	Limit:  10, // 10 requests
	Window: 1 * time.Second,
}

// rateLimitLuaScript executes atomic increment and expiration
const rateLimitLuaScript = `
local current = redis.call('incr', KEYS[1])
if current == 1 then
    redis.call('expire', KEYS[1], ARGV[1])
end
if current > tonumber(ARGV[2]) then
    return 0
end
return 1
`

// NewRateLimiterMiddleware creates a Redis-backed rate limiter for Anti-Bot protection
func NewRateLimiterMiddleware(client *redisclient.Client, cfg ...RateLimiterConfig) func(http.Handler) http.Handler {
	config := DefaultRateLimiterConfig
	if len(cfg) > 0 {
		config = cfg[0]
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if client == nil {
				next.ServeHTTP(w, r)
				return
			}

			// Key by Client IP Address
			ip := r.Header.Get("X-Forwarded-For")
			if ip == "" {
				ip = r.RemoteAddr
			}

			key := fmt.Sprintf("rate_limit:%s", ip)
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()

			windowSeconds := int(config.Window.Seconds())
			if windowSeconds <= 0 {
				windowSeconds = 1
			}

			res, err := client.Eval(ctx, rateLimitLuaScript, []string{key}, windowSeconds, config.Limit).Result()
			if err != nil {
				// Fail-open: if Redis has a transient issue, allow traffic rather than blocking all users
				next.ServeHTTP(w, r)
				return
			}

			allowed, ok := res.(int64)
			if !ok || allowed == 0 {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", strconv.Itoa(windowSeconds))
				w.Header().Set("X-RateLimit-Limit", strconv.Itoa(config.Limit))
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"error":       "rate limit exceeded, please slow down",
					"retry_after": windowSeconds,
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
