package service

import (
	"billing-service/internal/domain"
	"context"
	"errors"

	"github.com/google/uuid"
)

// BillingService — бизнес-логика квот.
type BillingService struct {
	repo QuotaRepository
}

// NewBillingService создаёт сервис.
func NewBillingService(repo QuotaRepository) *BillingService {
	return &BillingService{repo: repo}
}

// ReserveQuota резервирует единицу квоты под request_id. Если квоты нет, создаёт её на тарифе free.
// Ошибка ErrQuotaExceeded означает штатный отказ, остальные ошибки инфраструктурные.
func (s *BillingService) ReserveQuota(ctx context.Context, requestID, userID uuid.UUID) error {
	_, err := s.repo.GetQuota(ctx, userID)
	if err != nil {
		if errors.Is(err, domain.ErrQuotaNotFound) {
			if createErr := s.repo.CreateQuota(ctx, userID, "free"); createErr != nil {
				return createErr
			}
		} else {
			return err
		}
	}
	return s.repo.ReserveWithRecord(ctx, requestID, userID)
}

// ConfirmReservation фиксирует, что ссылка создана и бронь компенсировать не нужно.
func (s *BillingService) ConfirmReservation(ctx context.Context, requestID uuid.UUID) error {
	return s.repo.ConfirmReservation(ctx, requestID)
}
