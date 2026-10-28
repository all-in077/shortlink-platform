package service

import (
	"context"
	"log"
	"time"
)

// Sweeper периодически возвращает квоту за брони, которые не подтверждены в течение expireAfter:
// так сага компенсируется, если shortener не создал ссылку. Безопасен при нескольких экземплярах.
type Sweeper struct {
	repo          QuotaRepository
	checkInterval time.Duration
	expireAfter   time.Duration
}

// NewSweeper создаёт Sweeper.
func NewSweeper(repo QuotaRepository, checkInterval, expireAfter time.Duration) *Sweeper {
	return &Sweeper{
		repo:          repo,
		checkInterval: checkInterval,
		expireAfter:   expireAfter,
	}
}

// Start запускает цикл проверки до отмены контекста.
func (s *Sweeper) Start(ctx context.Context) {
	log.Println("Billing sweeper started...")
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Sweeper stopping...")
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

// sweep один проход: находит просроченные брони и компенсирует каждую. Ошибка одной брони
// не прерывает остальные.
func (s *Sweeper) sweep(ctx context.Context) {
	expired, err := s.repo.FindExpiredReservations(ctx, s.expireAfter)
	if err != nil {
		log.Printf("sweeper: failed to find expired reservations: %v", err)
		return
	}

	for _, res := range expired {
		if err := s.repo.CompensateReservation(ctx, res.RequestID, res.UserID); err != nil {
			log.Printf("sweeper: failed to compensate reservation %s: %v", res.RequestID, err)
			continue
		}
		log.Printf("sweeper: compensated expired reservation %s for user %s", res.RequestID, res.UserID)
	}
}
