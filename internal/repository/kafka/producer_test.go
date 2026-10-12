package kafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Patchanonss/flash-sale-engine/internal/domain"
)

func TestOrderPlacedEvent_JSONSerialization(t *testing.T) {
	event := domain.OrderPlacedEvent{
		EventID:    "evt-123",
		OrderID:    "order-456",
		UserID:     "user-789",
		ItemID:     1,
		Quantity:   2,
		TotalPrice: 998.00,
		Timestamp:  time.Now().Truncate(time.Millisecond),
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	var decoded domain.OrderPlacedEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal event: %v", err)
	}

	if decoded.OrderID != event.OrderID {
		t.Errorf("expected OrderID %s, got %s", event.OrderID, decoded.OrderID)
	}
	if decoded.UserID != event.UserID {
		t.Errorf("expected UserID %s, got %s", event.UserID, decoded.UserID)
	}
	if decoded.TotalPrice != event.TotalPrice {
		t.Errorf("expected TotalPrice %.2f, got %.2f", event.TotalPrice, decoded.TotalPrice)
	}
}
