-- Тарифы. limit_count = -1 означает безлимит.
-- Квота хранит копию лимита (снимок на момент создания), поэтому смена тарифа не меняет выданные квоты.
-- reservations: бронь под запрос саги; частичный индекс ускоряет поиск неподтверждённых для sweeper.
CREATE TABLE plans (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    limit_count BIGINT NOT NULL, -- -1 = unlimited
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

INSERT INTO plans (id, name, limit_count) VALUES
    ('free', 'Free', 50),
    ('pro', 'Pro', 5000),
    ('enterprise', 'Enterprise', -1);

CREATE TABLE quotas (
    user_id UUID PRIMARY KEY,
    plan_id VARCHAR(20) NOT NULL REFERENCES plans(id),
    used BIGINT NOT NULL DEFAULT 0,
    limit_count BIGINT NOT NULL,
    reset_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE reservations (
    request_id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES quotas(user_id),
    confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_reservations_pending ON reservations(reserved_at) WHERE confirmed = FALSE;