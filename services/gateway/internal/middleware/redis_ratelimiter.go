package middleware

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisRateLimiter — лимит запросов по IP, fixed window на INCR+EXPIRE. Счётчик общий
// для всех экземпляров gateway. На границе окон возможен всплеск до двух лимитов.
type RedisRateLimiter struct {
	client *redis.Client
	limit  int64
	window time.Duration
}

// NewRedisRateLimiter: limit запросов за окно window с одного IP.
func NewRedisRateLimiter(client *redis.Client, limit int64, window time.Duration) *RedisRateLimiter {
	return &RedisRateLimiter{client: client, limit: limit, window: window}
}

// allow увеличивает счётчик текущего окна; TxPipeline исполняет INCR и EXPIRE вместе,
// чтобы ключ не остался без TTL.
func (l *RedisRateLimiter) allow(ctx context.Context, ip string) (bool, error) {
	bucket := time.Now().UnixNano() / int64(l.window)
	key := fmt.Sprintf("rl:%s:%d", ip, bucket)

	pipe := l.client.TxPipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, l.window*2)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	return incr.Val() <= l.limit, nil
}

// Limit — middleware. При недоступном Redis пропускает запросы (fail-open): потеря защиты
// лучше, чем остановка всего сервиса.
func (l *RedisRateLimiter) Limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}

		ctx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
		defer cancel()

		ok, err := l.allow(ctx, ip)
		if err != nil {
			// fail-open: при недоступном Redis пропускаем, а не блокируем всех
			log.Printf("rate limiter redis error: %v", err)
			next.ServeHTTP(w, r)
			return
		}
		if !ok {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
