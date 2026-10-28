package domain

import "time"

// EventType — тип события в топике url-events.
type EventType string

const (
	EventURLCreated EventType = "url_created"
	EventURLVisited EventType = "url_visited"
)

// URLEvent — событие из топика url-events. Читаются только нужные сервису поля,
// остальные поля JSON (event_id, original_url и другие) игнорируются.
// VisitCount заполнен у url_visited и равен номеру перехода.
type URLEvent struct {
	EventType   EventType `json:"event_type"`
	ShortKey    string    `json:"short_key"`
	OriginalURL string    `json:"original_url"`
	VisitCount  int64     `json:"visit_count"`
	OccurredAt  time.Time `json:"occurred_at"`
}
