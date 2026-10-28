package domain

import (
	"errors"
	"time"
)

// ErrShortCodeTaken возвращается, если сгенерированный короткий код уже занят.
var ErrShortCodeTaken = errors.New("short code already taken")

// URL — короткая ссылка.
type URL struct {
	ID        int64
	LongURL   string
	ShortCode string
	CreatedAt time.Time
}

// OutboxEvent — строка таблицы outbox_events: событие, которое ещё нужно доставить в Kafka.
// Запись создаётся в одной транзакции с изменением данных (transactional outbox).
type OutboxEvent struct {
	ID        int64
	EventType string
	Payload   []byte
	Status    string
}

// OutboxPayload — тело события для Kafka. Содержит поля для обоих потребителей:
// Analytics читает event_id, event_type, short_key, original_url, created_at,
// Notification читает visit_count и occurred_at. Лишние поля каждый потребитель игнорирует.
type OutboxPayload struct {
	EventID     string    `json:"event_id"`
	EventType   string    `json:"event_type"`
	ShortKey    string    `json:"short_key"`
	OriginalURL string    `json:"original_url"`
	VisitCount  int64     `json:"visit_count"`
	CreatedAt   int64     `json:"created_at"`  // unix ms, нужно Analytics
	OccurredAt  time.Time `json:"occurred_at"` // нужно Notification
}
