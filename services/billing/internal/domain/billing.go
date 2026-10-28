package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// Ошибки домена: ErrQuotaExceeded — лимит тарифа исчерпан, ErrQuotaNotFound — у пользователя
// ещё нет квоты (создаётся при первом запросе), ErrPlanNotFound — неизвестный тариф.
var (
	ErrQuotaExceeded = errors.New("quota exceeded")
	ErrQuotaNotFound = errors.New("quota not found")
	ErrPlanNotFound  = errors.New("plan not found")
)

// Plan — тариф. Limit = -1 означает безлимит.
type Plan struct {
	ID        string // "free", "pro", "enterprise" — читаемый slug, не uuid
	Name      string
	Limit     int64 // -1 означает безлимит (enterprise)
	CreatedAt time.Time
}

// Quota — текущее состояние квоты пользователя. Limit копируется из тарифа при создании
// (снимок): смена лимита тарифа не затрагивает уже выданные квоты. ResetAt — начало следующего периода.
type Quota struct {
	UserID    uuid.UUID `db:"user_id"`
	PlanID    string    `db:"plan_id"`
	Used      int64     `db:"used"`
	Limit     int64     `db:"limit_count"`
	ResetAt   time.Time `db:"reset_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

// Reservation — бронь одной единицы квоты под запрос саги. Пока Confirmed=false,
// sweeper вправе её откатить.
type Reservation struct {
	RequestID  uuid.UUID `db:"request_id"`
	UserID     uuid.UUID `db:"user_id"`
	Confirmed  bool      `db:"confirmed"`
	ReservedAt time.Time `db:"reserved_at"`
}

// События саги — общие структуры сообщений Kafka
type QuotaRequestEvent struct {
	RequestID string    `json:"request_id"`
	UserID    string    `json:"user_id"`
	Timestamp time.Time `json:"timestamp"`
}

type QuotaResponseEvent struct {
	RequestID string `json:"request_id"`
	Approved  bool   `json:"approved"`
	Reason    string `json:"reason,omitempty"`
}

type ConfirmationEvent struct {
	RequestID string `json:"request_id"`
}
