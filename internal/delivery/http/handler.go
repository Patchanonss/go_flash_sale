package http

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"

	"github.com/Patchanonss/flash-sale-engine/internal/domain"
	"github.com/Patchanonss/flash-sale-engine/internal/usecase"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
)

type Handler struct {
	usecase     usecase.FlashSaleUsecase
	orderRepo   domain.OrderRepository
	wsHub       *WSHub
	rateLimiter func(http.Handler) http.Handler
}

func NewHandler(uc usecase.FlashSaleUsecase, orderRepo domain.OrderRepository, wsHub *WSHub) *Handler {
	return &Handler{
		usecase:   uc,
		orderRepo: orderRepo,
		wsHub:     wsHub,
	}
}

func (h *Handler) SetRateLimiter(rl func(http.Handler) http.Handler) {
	h.rateLimiter = rl
}

func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()

	// Global Middlewares
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	if os.Getenv("DISABLE_HTTP_LOG") != "true" {
		r.Use(middleware.Logger)
	}
	r.Use(middleware.Recoverer)

	// CORS for Vercel Frontend
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token", "Idempotency-Key"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	// API Routes
	r.Route("/api", func(api chi.Router) {
		if h.rateLimiter != nil {
			api.With(h.rateLimiter).Post("/orders", h.PlaceOrder)
		} else {
			api.Post("/orders", h.PlaceOrder)
		}
		api.Get("/items/{id}/stock", h.GetStock)
		api.Post("/items/{id}/reset", h.ResetStock)
		api.Get("/orders/recent", h.GetRecentOrders)
		api.Get("/health", h.HealthCheck)
	})

	// WebSocket Endpoint
	r.Get("/ws/stock", h.ServeWS)

	// Serve Static Web Frontend
	fs := http.FileServer(http.Dir("./web"))
	r.Handle("/*", fs)

	return r
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "flash-sale-engine"})
}

func (h *Handler) PlaceOrder(w http.ResponseWriter, r *http.Request) {
	var req domain.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ItemID == 0 {
		req.ItemID = 1 // Default item ID
	}
	if req.Quantity == 0 {
		req.Quantity = 1
	}
	if req.UserID == "" {
		reqID := middleware.GetReqID(r.Context())
		if reqID != "" {
			req.UserID = "user-" + reqID
		} else {
			req.UserID = "user-guest"
		}
	}

	order, remainingStock, err := h.usecase.PlaceOrder(r.Context(), &req)
	if err != nil {
		if err == domain.ErrOutOfStock {
			respondJSON(w, http.StatusConflict, map[string]interface{}{
				"error":           "item is out of stock",
				"remaining_stock": remainingStock,
			})
			return
		}
		if err == domain.ErrDuplicateOrder {
			respondJSON(w, http.StatusOK, map[string]interface{}{
				"message": "duplicate request detected, idempotency enforced",
			})
			return
		}
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Broadcast updated stock to all WebSocket clients
	if h.wsHub != nil {
		h.wsHub.BroadcastStock(req.ItemID, remainingStock)
	}

	respondJSON(w, http.StatusAccepted, map[string]interface{}{
		"message":         "order placed successfully and queued for processing",
		"order":           order,
		"remaining_stock": remainingStock,
	})
}

func (h *Handler) GetStock(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid item id")
		return
	}

	stock, err := h.usecase.GetItemStock(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, "stock not found")
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"item_id": id,
		"stock":   stock,
	})
}

type ResetStockRequest struct {
	Stock int `json:"stock"`
}

func (h *Handler) ResetStock(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid item id")
		return
	}

	var req ResetStockRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Stock <= 0 {
		req.Stock = 100 // Default to 100 stock
	}

	if err := h.usecase.ResetStock(r.Context(), id, req.Stock); err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Broadcast reset stock to WebSocket clients
	if h.wsHub != nil {
		h.wsHub.BroadcastStock(id, req.Stock)
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"message": "stock reset successfully",
		"item_id": id,
		"stock":   req.Stock,
	})
}

func (h *Handler) GetRecentOrders(w http.ResponseWriter, r *http.Request) {
	if h.orderRepo == nil {
		respondJSON(w, http.StatusOK, []interface{}{})
		return
	}

	orders, err := h.orderRepo.ListRecent(r.Context(), 20)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, orders)
}

func (h *Handler) ServeWS(w http.ResponseWriter, r *http.Request) {
	if h.wsHub == nil {
		http.Error(w, "WebSocket not enabled", http.StatusServiceUnavailable)
		return
	}

	currentStock, _ := h.usecase.GetItemStock(r.Context(), 1)
	h.wsHub.HandleWS(w, r, currentStock, 1)
}

func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}
