CREATE DATABASE IF NOT EXISTS analytics;

-- События ссылок: создание и переходы. Таблица только для добавления (append-only).
-- MergeTree и ORDER BY (event_type, created_at, short_key) ускоряют типичные отчёты:
-- фильтр по типу события, затем по времени и ссылке.
-- Выполняется один раз, при первой инициализации пустого тома ClickHouse.
CREATE TABLE IF NOT EXISTS analytics.url_events
(
    event_id     UUID,
    event_type   String,         -- 'url_created' или 'url_visited'
    short_key    String,
    original_url String,
    created_at   DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
ORDER BY (event_type, created_at, short_key);
