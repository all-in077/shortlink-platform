package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
	"url-shortener/internal/domain"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// Repository — работа с Postgres: ссылки и outbox.
type Repository struct {
	db *sql.DB
}

// NewURLRepository создаёт Repository.
func NewURLRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

// SaveWithOutbox сохраняет ссылку и событие outbox в одной транзакции: либо есть и то и другое,
// либо ничего. Если код уже занят, возвращает ErrShortCodeTaken (сервис повторит с новым кодом).
func (r *Repository) SaveWithOutbox(ctx context.Context, url domain.URL, event domain.OutboxEvent) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // Защита от паники или раннего return при ошибке

	queryURL := `INSERT INTO urls (long_url, short_code, created_at) VALUES ($1, $2, $3)`
	_, err = tx.ExecContext(ctx, queryURL, url.LongURL, url.ShortCode, url.CreatedAt)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			return domain.ErrShortCodeTaken
		}
		return err
	}

	queryOutbox := `INSERT INTO outbox_events (event_type, payload, status) VALUES ($1, $2, $3)`
	_, err = tx.ExecContext(ctx, queryOutbox, event.EventType, event.Payload, event.Status)
	if err != nil {
		return err
	}

	return tx.Commit()

}

// GetByCode возвращает ссылку по короткому коду. Если её нет, возвращается sql.ErrNoRows:
// хендлер по нему отдаёт 404.
func (r *Repository) GetByCode(ctx context.Context, code string) (domain.URL, error) {
	query := `SELECT id, long_url, short_code, created_at FROM urls WHERE short_code = $1`

	var url domain.URL
	err := r.db.QueryRowContext(ctx, query, code).Scan(
		&url.ID,
		&url.LongURL,
		&url.ShortCode,
		&url.CreatedAt,
	)
	if err != nil {
		return domain.URL{}, err
	}

	return url, nil
}

// ProcessPendingEvents берёт пачку pending-событий под FOR UPDATE SKIP LOCKED и держит
// блокировку всю транзакцию: пока handle публикует их в Kafka, другой воркер эти строки
// не возьмёт. Гарантия at-least-once: если сбой случится после публикации, но до COMMIT,
// событие уйдёт повторно, поэтому потребители обязаны терпеть дубли.
func (r *Repository) ProcessPendingEvents(ctx context.Context, limit int, handle func(events []domain.OutboxEvent) ([]int64, error)) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := `SELECT id, event_type, payload, status 
			FROM outbox_events 
			WHERE status = 'pending' 
			ORDER BY id ASC 
			LIMIT $1 
			FOR UPDATE SKIP LOCKED`

	rows, err := tx.QueryContext(ctx, query, limit)
	if err != nil {
		return err
	}

	events := make([]domain.OutboxEvent, 0)
	for rows.Next() {
		var event domain.OutboxEvent
		if err := rows.Scan(&event.ID, &event.EventType, &event.Payload, &event.Status); err != nil {
			rows.Close()
			return err
		}
		events = append(events, event)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	if len(events) == 0 {
		return tx.Commit() // ничего не делаем, но транзакцию закрыть надо
	}

	// Ошибка handle игнорируется осознанно: частичный успех отражён в processedIDs,
	// остальные события остаются pending и будут взяты на следующем тике.
	processedIDs, _ := handle(events)

	for _, id := range processedIDs {
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_events SET status = 'processed' WHERE id = $1`, id); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// RecordVisitWithOutbox увеличивает счётчик переходов и пишет событие url_visited в outbox
// в одной транзакции: счётчик и событие не могут разойтись.
func (r *Repository) RecordVisitWithOutbox(ctx context.Context, code, originalURL string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var visitCount int64
	err = tx.QueryRowContext(ctx,
		`UPDATE urls SET visit_count = visit_count + 1 WHERE short_code = $1 RETURNING visit_count`,
		code,
	).Scan(&visitCount)
	if err != nil {
		return err
	}

	now := time.Now()
	payload := domain.OutboxPayload{
		EventID:     uuid.New().String(),
		EventType:   "url_visited",
		ShortKey:    code,
		OriginalURL: originalURL,
		VisitCount:  visitCount,
		CreatedAt:   now.UnixMilli(),
		OccurredAt:  now,
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO outbox_events (event_type, payload, status) VALUES ($1, $2, $3)`,
		"url_visited", bytes, "pending",
	)
	if err != nil {
		return err
	}

	return tx.Commit()
}
