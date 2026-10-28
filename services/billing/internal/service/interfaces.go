package service

import (
	"billing-service/internal/domain"
	"context"
	"time"

	"github.com/google/uuid"
)

// QuotaRepository — хранилище квот и броней. Сервис зависит от интерфейса, поэтому тестируется на моке.
type QuotaRepository interface {
	GetQuota(ctx context.Context, userID uuid.UUID) (*domain.Quota, error)
	CreateQuota(ctx context.Context, userID uuid.UUID, planID string) error

	ReserveWithRecord(ctx context.Context, requestID, userID uuid.UUID) error
	ConfirmReservation(ctx context.Context, requestID uuid.UUID) error
	FindExpiredReservations(ctx context.Context, olderThan time.Duration) ([]domain.Reservation, error)
	CompensateReservation(ctx context.Context, requestID, userID uuid.UUID) error
}
