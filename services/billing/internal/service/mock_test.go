package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"billing-service/internal/domain"

	"github.com/google/uuid"
)

type mockRepo struct {
	mu sync.Mutex

	quotas       map[uuid.UUID]*domain.Quota
	reservations []domain.Reservation

	createdQuotas []string // planID
	reserveCalls  int
	compensated   []uuid.UUID

	reserveErr    error
	getQuotaErr   error
	createErr     error
	findErr       error
	compensateErr map[uuid.UUID]error
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		quotas:        map[uuid.UUID]*domain.Quota{},
		compensateErr: map[uuid.UUID]error{},
	}
}

func (m *mockRepo) GetQuota(ctx context.Context, userID uuid.UUID) (*domain.Quota, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getQuotaErr != nil {
		return nil, m.getQuotaErr
	}
	q, ok := m.quotas[userID]
	if !ok {
		return nil, domain.ErrQuotaNotFound
	}
	return q, nil
}

func (m *mockRepo) CreateQuota(ctx context.Context, userID uuid.UUID, planID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createErr != nil {
		return m.createErr
	}
	m.createdQuotas = append(m.createdQuotas, planID)
	m.quotas[userID] = &domain.Quota{UserID: userID, PlanID: planID, Limit: 50}
	return nil
}

func (m *mockRepo) ReserveWithRecord(ctx context.Context, requestID, userID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reserveCalls++
	return m.reserveErr
}

func (m *mockRepo) ConfirmReservation(ctx context.Context, requestID uuid.UUID) error {
	return nil
}

func (m *mockRepo) FindExpiredReservations(ctx context.Context, olderThan time.Duration) ([]domain.Reservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.findErr != nil {
		return nil, m.findErr
	}
	return m.reservations, nil
}

func (m *mockRepo) CompensateReservation(ctx context.Context, requestID, userID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.compensateErr[requestID]; err != nil {
		return err
	}
	m.compensated = append(m.compensated, requestID)
	return nil
}

var errBoom = errors.New("boom")
