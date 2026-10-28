package analytics

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Event — событие из топика url-events (url_created или url_visited).
// Поля соответствуют OutboxPayload в shortener; лишние поля JSON игнорируются.
type Event struct {
	EventID     uuid.UUID `json:"event_id"`
	EventType   string    `json:"event_type"`
	ShortKey    string    `json:"short_key"`
	OriginalURL string    `json:"original_url"`
	CreatedAt   int64     `json:"created_at"` // Unix-время в миллисекундах
}

// Repository — запись событий в ClickHouse.
type Repository struct {
	conn driver.Conn
}

// NewRepository подключается к ClickHouse по нативному протоколу (порт 9000)
// и проверяет соединение, чтобы сервис не стартовал с неработающей базой.
func NewRepository(addr, dbName, user, password string) (*Repository, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: dbName,
			Username: user,
			Password: password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to clickhouse: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("clickhouse ping failed: %w", err)
	}

	return &Repository{conn: conn}, nil
}

// InsertBatch отправляет пачку событий в ClickHouse одним запросом.
// Построчные вставки здесь были бы антипаттерном: каждая создаёт отдельный кусок данных на диске.
// Повторная вставка тех же событий создаёт дубли (см. «Known limitations»: ReplacingMergeTree).
func (r *Repository) InsertBatch(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO analytics.url_events (event_id, event_type, short_key, original_url, created_at)")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, e := range events {
		err := batch.Append(
			e.EventID,
			e.EventType,
			e.ShortKey,
			e.OriginalURL,
			e.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch to clickhouse: %w", err)
	}

	return nil
}

// Ping проверяет доступность ClickHouse (используется в /readyz).
func (r *Repository) Ping(ctx context.Context) error {
	return r.conn.Ping(ctx)
}
