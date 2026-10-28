package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"
	"url-shortener/internal/domain"

	"github.com/google/uuid"
)

// URLService — бизнес-логика ссылок.
type URLService struct {
	repo  URLRepository
	cache URLCache
}

// NewURLService создаёт сервис.
func NewURLService(repo URLRepository, cache URLCache) *URLService {
	return &URLService{repo: repo,
		cache: cache}
}

// GetByCode возвращает ссылку: сначала из кэша, при промахе из БД с прогревом кэша.
// Ошибка записи в кэш игнорируется: кэш вторичен.
func (s *URLService) GetByCode(ctx context.Context, code string) (domain.URL, error) {
	if longURL, err := s.cache.Get(ctx, code); err == nil && longURL != "" {
		return domain.URL{
			ShortCode: code,
			LongURL:   longURL,
		}, nil
	}
	url, err := s.repo.GetByCode(ctx, code)

	if err != nil {
		return domain.URL{}, err
	}

	_ = s.cache.Set(ctx, code, url.LongURL, 24*time.Hour)

	return url, nil
}

// CreateShortURL создаёт ссылку и событие url_created в одной транзакции.
// Короткий код случайный; при коллизии (редкой) повторяет с новым кодом до трёх попыток.
func (uc *URLService) CreateShortURL(ctx context.Context, longURL string) (domain.URL, error) {
	const maxAttempts = 3

	for attempt := 0; attempt < maxAttempts; attempt++ {
		shortCode, err := generateShortCode(6)
		if err != nil {
			return domain.URL{}, fmt.Errorf("failed to generate short code: %w", err)
		}

		newURL := domain.URL{
			LongURL:   longURL,
			ShortCode: shortCode,
			CreatedAt: time.Now(),
		}

		payload := domain.OutboxPayload{
			EventID:     uuid.New().String(),
			EventType:   "url_created",
			ShortKey:    shortCode,
			OriginalURL: longURL,
			CreatedAt:   newURL.CreatedAt.UnixMilli(),
			OccurredAt:  newURL.CreatedAt,
		}
		bytes, err := json.Marshal(payload)
		if err != nil {
			return domain.URL{}, fmt.Errorf("failed to marshal outbox event payload: %w", err)
		}

		newEvent := domain.OutboxEvent{
			EventType: "url_created",
			Payload:   bytes,
			Status:    "pending",
		}

		err = uc.repo.SaveWithOutbox(ctx, newURL, newEvent)
		if err == nil {
			return newURL, nil
		}
		if !errors.Is(err, domain.ErrShortCodeTaken) {
			return domain.URL{}, err
		}
		// коллизия: пробуем со следующим кодом
	}

	return domain.URL{}, fmt.Errorf("failed to generate unique short code after %d attempts", maxAttempts)
}

// RecordVisit учитывает переход: увеличивает счётчик и пишет событие url_visited.
func (uc *URLService) RecordVisit(ctx context.Context, code string, originalURL string) error {
	return uc.repo.RecordVisitWithOutbox(ctx, code, originalURL)
}

// generateShortCode генерирует код из [a-zA-Z0-9] через crypto/rand: коды непредсказуемы,
// их нельзя перебрать по порядку.
func generateShortCode(length int) (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", err
		}
		b[i] = charset[num.Int64()]
	}
	return string(b), nil
}
