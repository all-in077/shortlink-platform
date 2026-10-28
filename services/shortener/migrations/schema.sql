CREATE TABLE IF NOT EXISTS urls (
    id BIGSERIAL PRIMARY KEY,
    short_code VARCHAR(10) NOT NULL UNIQUE,
    long_url TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Частичный индекс: воркер ищет только pending, обработанные записи в индекс не попадают.
CREATE INDEX IF NOT EXISTS idx_outbox_events_status ON outbox_events(status) WHERE status = 'pending';

-- Счётчик переходов, по нему Notification определяет достижение порога.
ALTER TABLE urls ADD COLUMN IF NOT EXISTS visit_count BIGINT NOT NULL DEFAULT 0;