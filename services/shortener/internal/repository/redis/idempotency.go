package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// IdempotencyStore хранит состояние запросов с Idempotency-Key.
// Ключ в Redis либо "processing" (запрос выполняется), либо готовый JSON-ответ.
type IdempotencyStore struct {
	client *redis.Client
}

// NewIdempotencyStore создаёт хранилище.
func NewIdempotencyStore(client *redis.Client) *IdempotencyStore {
	return &IdempotencyStore{client: client}
}

// processingMarker — значение ключа, пока запрос выполняется.
const processingMarker = "processing"

// Acquire пытается занять ключ. acquired=true: мы первые.
// Иначе existing содержит "processing" или сохранённый результат.
func (s *IdempotencyStore) Acquire(ctx context.Context, key string, ttl time.Duration) (acquired bool, existing string, err error) {
	ok, err := s.client.SetNX(ctx, "idem:"+key, processingMarker, ttl).Result()
	if err != nil {
		return false, "", err
	}
	if ok {
		return true, "", nil
	}
	val, err := s.client.Get(ctx, "idem:"+key).Result()
	if err == redis.Nil {
		// ключ истёк между SetNX и Get: пусть клиент повторит
		return false, processingMarker, nil
	}
	if err != nil {
		return false, "", err
	}
	return false, val, nil
}

// Save сохраняет итоговый ответ, чтобы повторный запрос получил его без повторной саги.
func (s *IdempotencyStore) Save(ctx context.Context, key, result string, ttl time.Duration) error {
	return s.client.Set(ctx, "idem:"+key, result, ttl).Err()
}

// Release удаляет ключ после неуспешной обработки, чтобы клиент мог повторить запрос.
func (s *IdempotencyStore) Release(ctx context.Context, key string) error {
	return s.client.Del(ctx, "idem:"+key).Err()
}

// IsProcessing сообщает, что запрос с этим ключом ещё выполняется.
func IsProcessing(v string) bool { return v == processingMarker }
