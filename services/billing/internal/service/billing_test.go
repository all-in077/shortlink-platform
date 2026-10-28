package service

import (
	"context"
	"errors"
	"testing"

	"billing-service/internal/domain"

	"github.com/google/uuid"
)

func TestReserveQuota(t *testing.T) {
	tests := []struct {
		name        string
		existing    bool
		setup       func(r *mockRepo)
		wantErr     error
		wantCreated int
		wantReserve int
	}{
		{
			name:        "existing quota is reserved without creating",
			existing:    true,
			wantReserve: 1,
		},
		{
			name:        "missing quota is created on the free plan first",
			existing:    false,
			wantCreated: 1,
			wantReserve: 1,
		},
		{
			name:        "limit exceeded is passed through",
			existing:    true,
			setup:       func(r *mockRepo) { r.reserveErr = domain.ErrQuotaExceeded },
			wantErr:     domain.ErrQuotaExceeded,
			wantReserve: 1,
		},
		{
			name:        "create failure stops before reserving",
			existing:    false,
			setup:       func(r *mockRepo) { r.createErr = errBoom },
			wantErr:     errBoom,
			wantCreated: 0,
			wantReserve: 0,
		},
		{
			name:        "infrastructure error on lookup is not treated as missing quota",
			existing:    true,
			setup:       func(r *mockRepo) { r.getQuotaErr = errBoom },
			wantErr:     errBoom,
			wantCreated: 0,
			wantReserve: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newMockRepo()
			userID := uuid.New()
			if tt.existing {
				repo.quotas[userID] = &domain.Quota{UserID: userID, PlanID: "pro"}
			}
			if tt.setup != nil {
				tt.setup(repo)
			}
			svc := NewBillingService(repo)

			err := svc.ReserveQuota(context.Background(), uuid.New(), userID)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if len(repo.createdQuotas) != tt.wantCreated {
				t.Fatalf("created quotas = %d, want %d", len(repo.createdQuotas), tt.wantCreated)
			}
			if repo.reserveCalls != tt.wantReserve {
				t.Fatalf("reserve calls = %d, want %d", repo.reserveCalls, tt.wantReserve)
			}
			if tt.wantCreated == 1 && repo.createdQuotas[0] != "free" {
				t.Fatalf("default plan = %q, want free", repo.createdQuotas[0])
			}
		})
	}
}
