package service

import (
	"context"
	"time"
	"url-shortener/internal/domain"
)

// URLRepository — хранилище ссылок. Сервис зависит от интерфейса, а не от Postgres.
type URLRepository interface {
	SaveWithOutbox(ctx context.Context, url domain.URL, event domain.OutboxEvent) error
	GetByCode(ctx context.Context, code string) (domain.URL, error)
	RecordVisitWithOutbox(ctx context.Context, code, originalURL string) error
}

// URLCache — кэш ссылок; его сбои не должны ломать работу сервиса.
type URLCache interface {
	Get(ctx context.Context, code string) (string, error)
	Set(ctx context.Context, code string, longURL string, ttl time.Duration) error
}
