package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"notification-service/internal/domain"
	"notification-service/internal/metrics"
	"notification-service/internal/sender"
)

// IdempotencyStore запоминает, по каким событиям вебхук уже отправлен, чтобы повторная
// доставка из Kafka не привела к дублю уведомления. Хранится в памяти процесса:
// после перезапуска память пуста (см. «Known limitations»).
type IdempotencyStore struct {
	mu   sync.RWMutex
	seen map[string]time.Time
}

// NewIdempotencyStore создаёт пустое хранилище.
func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{seen: make(map[string]time.Time)}
}

// IsProcessed сообщает, что ключ уже отмечен как обработанный.
func (s *IdempotencyStore) IsProcessed(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.seen[key]
	return ok
}

// MarkProcessed отмечает ключ обработанным с текущим временем.
func (s *IdempotencyStore) MarkProcessed(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[key] = time.Now()
}

// Cleanup удаляет ключи старше olderThan и возвращает их число.
// Без очистки память росла бы бесконечно.
func (s *IdempotencyStore) Cleanup(olderThan time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	removed := 0
	for key, ts := range s.seen {
		if ts.Before(cutoff) {
			delete(s.seen, key)
			removed++
		}
	}
	return removed
}

// StartCleanup раз в interval удаляет ключи старше ttl до отмены контекста.
func (s *IdempotencyStore) StartCleanup(ctx context.Context, interval, ttl time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n := s.Cleanup(ttl); n > 0 {
				log.Printf("idempotency store: removed %d expired keys", n)
			}
		}
	}
}

// Notifier решает, нужно ли уведомление по событию, и отправляет вебхук.
type Notifier struct {
	sender    *sender.Sender
	store     *IdempotencyStore
	threshold int64
}

// NewNotifier создаёт Notifier. threshold — число переходов, при котором уходит вебхук.
func NewNotifier(sender *sender.Sender, threshold int64) *Notifier {
	return &Notifier{sender: sender, store: NewIdempotencyStore(), threshold: threshold}
}

// HandleEvent обрабатывает одно событие. Уведомление отправляется один раз, при переходе,
// на котором счётчик ссылки в точности равен порогу. Номер перехода присваивает shortener
// внутри транзакции (UPDATE ... RETURNING), поэтому такое событие у каждой ссылки одно.
// Ошибка возвращается только при сбое отправки: consumer повторит обработку.
func (n *Notifier) HandleEvent(ctx context.Context, event domain.URLEvent) error {
	metrics.EventsProcessedTotal.WithLabelValues(string(event.EventType)).Inc()

	if event.EventType != domain.EventURLVisited {
		return nil
	}
	if event.VisitCount != n.threshold {
		return nil
	}

	key := fmt.Sprintf("%s:%s:%d", event.EventType, event.ShortKey, event.VisitCount)
	if n.store.IsProcessed(key) {
		log.Printf("duplicate skipped: %s", key)
		return nil
	}

	payload := sender.WebhookPayload{
		EventType:  string(event.EventType),
		ShortKey:   event.ShortKey,
		VisitCount: event.VisitCount,
		Message:    fmt.Sprintf("Ссылка %s набрала %d переходов", event.ShortKey, event.VisitCount),
	}

	if err := n.sender.Send(ctx, payload); err != nil {
		metrics.WebhooksFailedTotal.Inc()
		return err
	}

	metrics.WebhooksSentTotal.Inc()
	n.store.MarkProcessed(key)
	log.Printf("notification sent for %s", key)
	return nil
}

// StartCleanup запускает фоновую очистку хранилища идемпотентности.
func (n *Notifier) StartCleanup(ctx context.Context, interval, ttl time.Duration) {
	n.store.StartCleanup(ctx, interval, ttl)
}
