package http

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for demo & Vercel deployment
	},
}

type StockUpdateMessage struct {
	ItemID         int64     `json:"item_id"`
	RemainingStock int       `json:"remaining_stock"`
	Timestamp      time.Time `json:"timestamp"`
}

type WSHub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan StockUpdateMessage
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.Mutex
}

func NewWSHub() *WSHub {
	return &WSHub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan StockUpdateMessage, 256),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
	}
}

func (h *WSHub) Run() {
	for {
		select {
		case conn := <-h.register:
			h.mu.Lock()
			h.clients[conn] = true
			h.mu.Unlock()

		case conn := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[conn]; ok {
				delete(h.clients, conn)
				conn.Close()
			}
			h.mu.Unlock()

		case msg := <-h.broadcast:
			payload, err := json.Marshal(msg)
			if err != nil {
				continue
			}

			h.mu.Lock()
			for conn := range h.clients {
				err := conn.WriteMessage(websocket.TextMessage, payload)
				if err != nil {
					conn.Close()
					delete(h.clients, conn)
				}
			}
			h.mu.Unlock()
		}
	}
}

func (h *WSHub) BroadcastStock(itemID int64, remainingStock int) {
	h.broadcast <- StockUpdateMessage{
		ItemID:         itemID,
		RemainingStock: remainingStock,
		Timestamp:      time.Now(),
	}
}

func (h *WSHub) HandleWS(w http.ResponseWriter, r *http.Request, initialStock int, itemID int64) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WebSocket] Upgrade error: %v", err)
		return
	}

	h.register <- conn

	// Send initial stock snapshot immediately
	initialMsg, _ := json.Marshal(StockUpdateMessage{
		ItemID:         itemID,
		RemainingStock: initialStock,
		Timestamp:      time.Now(),
	})
	_ = conn.WriteMessage(websocket.TextMessage, initialMsg)

	// Keep connection alive until client disconnects
	go func() {
		defer func() {
			h.unregister <- conn
		}()
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				break
			}
		}
	}()
}
