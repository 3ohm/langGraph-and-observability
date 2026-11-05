package orders

import (
	"context"
	"log/slog"
	"sync"
)

// Reservation holds stock of one SKU for one order.
type Reservation struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
}

// Service keeps the reserved quantity of each SKU, per order.
type Service struct {
	mu       sync.Mutex
	reserved map[string]map[string]int
	pending  chan Reservation
}

// NewService returns a Service for the SKUs of the catalog snapshot.
func NewService(skus []string) *Service {
	reserved := make(map[string]map[string]int, len(skus))
	for _, sku := range skus {
		reserved[sku] = make(map[string]int)
	}
	return &Service{reserved: reserved, pending: make(chan Reservation, 256)}
}

// Submit queues a reservation. Run applies it.
func (s *Service) Submit(r Reservation) {
	s.pending <- r
}

// Run applies queued reservations until ctx is done.
func (s *Service) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r := <-s.pending:
			s.reserve(r)
		}
	}
}

func (s *Service) reserve(r Reservation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bySKU := s.reserved[r.SKU]
	bySKU[r.OrderID] += r.Qty
	slog.Info("reserved stock", "order", r.OrderID, "sku", r.SKU, "qty", r.Qty)
}
