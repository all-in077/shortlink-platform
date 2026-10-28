package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// URLCache — кэш коротких ссылок в Redis: снимает с Postgres нагрузку на чтение редиректов.
type URLCache struct {
	client *redis.Client
}

// NewURLCache создаёт кэш.
func NewURLCache(client *redis.Client) *URLCache {
	return &URLCache{client: client}
}

// Get возвращает длинный URL по коду. При промахе возвращается redis.Nil,
// сервис воспринимает любую ошибку как промах и идёт в базу.
func (c *URLCache) Get(ctx context.Context, code string) (string, error) {
	// В Redis ключи лучше делать с префиксом, например "url:aB3x9"
	key := "url:" + code
	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		// если ключа нет, redis вернет redis.Nil
		return "", err
	}
	return val, nil
}

func (c *URLCache) Set(ctx context.Context, code string, longURL string, ttl time.Duration) error {
	key := "url:" + code
	return c.client.Set(ctx, key, longURL, ttl).Err()
}
