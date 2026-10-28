package service

import (
	"context"
	"testing"
	"time"

	"billing-service/internal/domain"

	"github.com/google/uuid"
)

func TestSweepCompensatesAllExpired(t *testing.T) {
	repo := newMockRepo()
	r1, r2 := uuid.New(), uuid.New()
	repo.reservations = []domain.Reservation{
		{RequestID: r1, UserID: uuid.New()},
		{RequestID: r2, UserID: uuid.New()},
	}
	s := NewSweeper(repo, time.Second, 10*time.Second)

	s.sweep(context.Background())

	if len(repo.compensated) != 2 {
		t.Fatalf("compensated = %v, want both reservations", repo.compensated)
	}
}

func TestSweepOneFailureDoesNotStopTheRest(t *testing.T) {
	repo := newMockRepo()
	bad, good := uuid.New(), uuid.New()
	repo.reservations = []domain.Reservation{
		{RequestID: bad, UserID: uuid.New()},
		{RequestID: good, UserID: uuid.New()},
	}
	repo.compensateErr[bad] = errBoom
	s := NewSweeper(repo, time.Second, 10*time.Second)

	s.sweep(context.Background())

	if len(repo.compensated) != 1 || repo.compensated[0] != good {
		t.Fatalf("compensated = %v, want only the good one", repo.compensated)
	}
}

func TestSweepSearchFailureCompensatesNothing(t *testing.T) {
	repo := newMockRepo()
	repo.findErr = errBoom
	s := NewSweeper(repo, time.Second, 10*time.Second)

	s.sweep(context.Background())

	if len(repo.compensated) != 0 {
		t.Fatalf("compensated = %v, want none", repo.compensated)
	}
}

func TestSweeperStopsOnContextCancel(t *testing.T) {
	s := NewSweeper(newMockRepo(), 10*time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		s.Start(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sweeper did not stop after context cancel")
	}
}
