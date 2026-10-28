package postgres

import (
	"billing-service/internal/domain"
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// BillingRepository — работа с квотами и бронями в Postgres.
type BillingRepository struct {
	db *sqlx.DB
}

// NewBillingRepository создаёт репозиторий.
func NewBillingRepository(db *sqlx.DB) *BillingRepository {
	return &BillingRepository{db: db}
}

// GetQuota возвращает квоту пользователя или ErrQuotaNotFound.
func (r *BillingRepository) GetQuota(ctx context.Context, userID uuid.UUID) (*domain.Quota, error) {
	query := `SELECT user_id, plan_id, used, limit_count, reset_at, updated_at FROM quotas WHERE user_id=$1`

	var quota domain.Quota
	err := r.db.GetContext(ctx, &quota, query, userID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrQuotaNotFound
		}
		return nil, err
	}

	return &quota, nil

}

// CreateQuota создаёт квоту на тарифе planID, копируя его лимит. ON CONFLICT DO NOTHING
// снимает гонку, когда два первых запроса одного пользователя создают квоту одновременно.
func (r *BillingRepository) CreateQuota(ctx context.Context, userID uuid.UUID, planID string) error {
	// Достаём лимит тарифа, чтобы сделать "снапшот" в quotas.limit_count
	var limit int64
	err := r.db.GetContext(ctx, &limit, `SELECT limit_count FROM plans WHERE id = $1`, planID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrPlanNotFound
		}
		return err
	}

	resetAt := nextMonthStart()

	query := `INSERT INTO quotas (user_id, plan_id, used, limit_count, reset_at, updated_at)
	          VALUES ($1, $2, 0, $3, $4, now())
	          ON CONFLICT (user_id) DO NOTHING`

	_, err = r.db.ExecContext(ctx, query, userID, planID, limit, resetAt)
	return err
}

// nextMonthStart возвращает начало следующего месяца в UTC (в декабре time.Date переходит на январь следующего года).
func nextMonthStart() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
}

// ReserveWithRecord резервирует квоту под request_id. Метод идемпотентен:
// повторная доставка того же запроса не списывает квоту второй раз.
func (r *BillingRepository) ReserveWithRecord(ctx context.Context, requestID, userID uuid.UUID) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Бронь идёт первой: PRIMARY KEY по request_id отсекает повторную доставку.
	res, err := tx.ExecContext(ctx,
		`INSERT INTO reservations (request_id, user_id, confirmed) VALUES ($1, $2, false)
		 ON CONFLICT (request_id) DO NOTHING`, requestID, userID)
	if err != nil {
		return err
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		return nil // уже забронировано при предыдущей доставке
	}

	// limit_count < 0 означает безлимитный тариф (enterprise).
	res, err = tx.ExecContext(ctx,
		`UPDATE quotas SET used = used + 1, updated_at = now()
		 WHERE user_id = $1 AND (limit_count < 0 OR used < limit_count)`,
		userID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrQuotaExceeded // откат транзакции убирает и вставленную бронь
	}

	return tx.Commit()
}

// ConfirmReservation помечает бронь подтверждённой. Идемпотентно.
func (r *BillingRepository) ConfirmReservation(ctx context.Context, requestID uuid.UUID) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE reservations SET confirmed = true WHERE request_id = $1`, requestID)
	return err
}

// FindExpiredReservations возвращает неподтверждённые брони старше olderThan.
func (r *BillingRepository) FindExpiredReservations(ctx context.Context, olderThan time.Duration) ([]domain.Reservation, error) {
	var reservations []domain.Reservation
	err := r.db.SelectContext(ctx, &reservations,
		`SELECT request_id, user_id, confirmed, reserved_at FROM reservations
		 WHERE confirmed = false AND reserved_at < $1`,
		time.Now().Add(-olderThan))
	return reservations, err
}

// CompensateReservation возвращает квоту за неподтверждённую бронь. DELETE выступает замком:
// если строку уже удалил другой экземпляр sweeper'а, квота повторно не возвращается.
func (r *BillingRepository) CompensateReservation(ctx context.Context, requestID, userID uuid.UUID) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Сначала пытаемся удалить запись — это наш "замок".
	// Если удаление реально затронуло строку — значит мы первые, кто это обрабатывает.
	res, err := tx.ExecContext(ctx, `DELETE FROM reservations WHERE request_id = $1`, requestID)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		// Кто-то другой (второй инстанс sweeper'а) уже скомпенсировал эту резервацию.
		// Ничего не делаем, транзакция откатится через defer (там и делать нечего — DELETE уже "промазал").
		return nil
	}

	// Мы реально первые — теперь безопасно возвращаем квоту.
	_, err = tx.ExecContext(ctx,
		`UPDATE quotas SET used = used - 1, updated_at = now() WHERE user_id = $1 AND used > 0`, userID)
	if err != nil {
		return err
	}

	return tx.Commit()
}
