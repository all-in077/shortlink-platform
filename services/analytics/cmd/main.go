package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	analytics "analytics-service/internal"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Точка входа analytics: читает события из Kafka и пачками пишет их в ClickHouse.
// Параллельно отдаёт метрики и пробы по HTTP. Остановка по SIGINT/SIGTERM корректная:
// consumer успевает записать накопленный буфер.
func main() {
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaTopic := getEnv("KAFKA_TOPIC", "url-events")
	kafkaGroupID := getEnv("KAFKA_GROUP_ID", "analytics-group")

	clickhouseAddr := getEnv("CLICKHOUSE_ADDR", "localhost:9000")
	clickhouseDB := getEnv("CLICKHOUSE_DB", "analytics")
	clickhouseUser := getEnv("CLICKHOUSE_USER", "default")
	clickhousePassword := getEnv("CLICKHOUSE_PASSWORD", "")

	httpPort := getEnv("HTTP_PORT", ":8082")

	repo, err := newRepositoryWithRetry(clickhouseAddr, clickhouseDB, clickhouseUser, clickhousePassword)
	if err != nil {
		log.Fatalf("failed to init clickhouse repository: %v", err)
	}

	consumer := analytics.NewConsumer(kafkaBrokers, kafkaTopic, kafkaGroupID, repo)

	// Отмена контекста останавливает consumer: он дозаписывает буфер и выходит.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	consumerDone := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(consumerDone)
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	// healthz: процесс жив (liveness). readyz: доступен ClickHouse (readiness).
	// Разделены намеренно: недоступная БД не должна приводить к перезапуску пода.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := repo.Ping(pingCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("clickhouse unavailable"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})

	httpServer := &http.Server{
		Addr:              httpPort,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second, // защита от медленных клиентов
	}

	go func() {
		log.Printf("HTTP server (metrics/health) listening on %s", httpPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-sigCh
	log.Println("Shutdown signal received, stopping analytics service...")

	// Сначала останавливаем consumer и ждём, пока он запишет буфер,
	// и только потом HTTP-сервер.
	cancel()
	<-consumerDone

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("http server shutdown error: %v", err)
	}

	log.Println("Analytics service stopped gracefully")
}

// newRepositoryWithRetry подключается к ClickHouse с повторами: при холодном старте стека
// (docker compose, Kubernetes) база и DNS могут быть доступны не сразу.
func newRepositoryWithRetry(addr, db, user, password string) (*analytics.Repository, error) {
	const attempts = 10
	var lastErr error
	for i := 1; i <= attempts; i++ {
		repo, err := analytics.NewRepository(addr, db, user, password)
		if err == nil {
			return repo, nil
		}
		lastErr = err
		log.Printf("clickhouse is not ready (attempt %d/%d): %v", i, attempts, err)
		time.Sleep(3 * time.Second)
	}
	return nil, lastErr
}

// getEnv возвращает переменную окружения или значение по умолчанию.
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
