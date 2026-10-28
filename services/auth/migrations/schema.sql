-- refresh_tokens: ON DELETE CASCADE удаляет токены вместе с пользователем.
-- UNIQUE по token создаёт индекс, поэтому поиск по строке токена быстрый;
-- индекс по user_id нужен для отзыва всех сессий пользователя.

CREATE TABLE users(
    id UUID PRIMARY KEY,

    email VARCHAR(254) NOT NULL UNIQUE,
    password_hash VARCHAR(60) NOT NULL,

    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL


);


CREATE TABLE refresh_tokens(
    id BIGSERIAL PRIMARY KEY,
    token VARCHAR(240) NOT NULL UNIQUE,
    user_id UUID NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    revoked BOOLEAN DEFAULT FALSE NOT NULL,

    CONSTRAINT fk_user_token
        FOREIGN KEY (user_id)
        REFERENCES users(id)
        ON DELETE CASCADE
);

CREATE INDEX idx_refresh_tokens_user_id ON refresh_tokens(user_id);