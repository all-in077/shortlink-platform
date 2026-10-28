package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gateway-service/internal/client"
	httpDelivery "gateway-service/internal/delivery/http"
	"gateway-service/internal/middleware"

	"github.com/redis/go-redis/v9"
)

// getEnv возвращает переменную окружения или значение по умолчанию.
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Точка входа gateway: единая точка входа для клиентов. Лимит запросов по IP (Redis),
// проверка JWT через auth по gRPC, проксирование в shortener.
func main() {

	// HTTP_PORT — на чем слушает Gateway (клиенты стучатся сюда).
	// SHORTENER_URL — куда проксировать. Используем имя сервиса из docker-compose,
	// потому что внутри Docker-сети контейнеры видят друг друга по имени.
	httpPort := getEnv("HTTP_PORT", ":8082")
	shortenerURL := getEnv("SHORTENER_URL", "http://shortener:8080")

	// NewRouter создает Chi + ReverseProxy в Shortener + RateLimiter + /health.
	// Это единственное место, где мы "соединяем" слои Gateway.
	authClient, err := client.NewAuthClient(getEnv("AUTH_GRPC_ADDR", "auth:50051"))
	if err != nil {
		log.Fatalf("failed to create auth client: %v", err)
	}
	defer authClient.Close()

	rdb := redis.NewClient(&redis.Options{Addr: getEnv("REDIS_ADDR", "redis:6379")})
	defer rdb.Close()
	// 20 запросов в секунду с одного IP; счётчик в Redis общий для всех экземпляров gateway.
	limiter := middleware.NewRedisRateLimiter(rdb, 20, time.Second)

	authHTTPURL := getEnv("AUTH_HTTP_URL", "http://auth:8080")
	router := httpDelivery.NewRouter(authClient, shortenerURL, authHTTPURL, limiter)

	// ReadTimeout / WriteTimeout защищают от "медленных клиентов" (slowloris-атак).
	srv := &http.Server{
		Addr:         httpPort,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// main должен продолжить выполнение, чтобы установить обработчик сигналов.
	go func() {
		log.Printf("Gateway started on %s → proxying to %s", httpPort, shortenerURL)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("gateway error: %v", err)
		}
	}()

	// Блокируем main до SIGINT (Ctrl+C) или SIGTERM (Docker/Kubernetes stop).
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("shutting down gateway...")

	// Даем серверу 5 секунд на завершение текущих запросов.
	// Если не успеет — Shutdown вернет ошибку, но мы не паникуем, просто логируем.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("gateway shutdown error: %v", err)
	}

	log.Println("gateway stopped")
}
