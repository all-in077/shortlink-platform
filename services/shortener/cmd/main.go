package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"

	delivery "url-shortener/internal/delivery/http"
	"url-shortener/internal/metrics"
	"url-shortener/internal/pending"
	postgresRepo "url-shortener/internal/repository/postgres"
	redisRepo "url-shortener/internal/repository/redis"
	"url-shortener/internal/saga"
	"url-shortener/internal/service"
	"url-shortener/internal/worker"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Точка входа shortener: собирает зависимости, запускает HTTP-сервер,
// outbox-воркер и consumer ответов саги, при SIGINT/SIGTERM завершается корректно.
func main() {
	// Конфиг из env; дефолты рассчитаны на docker compose.
	pgDSN := getEnv("DATABASE_URL", "postgres://postgres:postgres@postgres:5432/url_shortener?sslmode=disable")
	redisAddr := getEnv("REDIS_ADDR", "redis:6379")
	kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:29092")
	baseURL := getEnv("BASE_URL", "http://localhost:8081/r")
	httpAddr := getEnv("HTTP_ADDR", ":8080")
	sagaGroupID := getEnv("SAGA_CONSUMER_GROUP", "shortener-saga-group")

	// В k8s у каждого пода своя consumer group: ответ саги должен попасть именно в тот под,
	// в памяти которого ждёт хендлер (см. pending.Registry). POD_NAME приходит из downward API.
	if pod := os.Getenv("POD_NAME"); pod != "" {
		sagaGroupID = sagaGroupID + "-" + pod
	}

	// ========== Postgres ==========
	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Postgres ping failed: %v", err)
	}
	log.Println("Connected to Postgres successfully!")

	// ========== Redis ==========
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis ping failed: %v", err)
	}
	log.Println("Connected to Redis successfully!")

	// ========== Сага: producer, registry, consumer ==========
	brokersList := strings.Split(kafkaBrokers, ",")
	sagaProducer := saga.NewProducer(brokersList)
	registry := pending.NewRegistry()
	responseConsumer := saga.NewResponseConsumer(brokersList, sagaGroupID, registry)

	// ========== Сборка слоёв ==========
	repo := postgresRepo.NewURLRepository(db)
	cache := redisRepo.NewURLCache(rdb)
	svc := service.NewURLService(repo, cache)
	idem := redisRepo.NewIdempotencyStore(rdb)
	handler := delivery.NewHandler(svc, baseURL, sagaProducer, registry, 3*time.Second, idem)

	// ========== Роутер ==========
	r := chi.NewRouter()
	r.Use(metrics.PrometheusMiddleware)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Post("/api/v1/shorten", handler.CreateShortURL)
	r.Get("/r/{code}", handler.Redirect)
	r.Handle("/metrics", promhttp.Handler())

	// healthz: процесс жив (liveness). readyz: доступны зависимости (readiness).
	// Разделены намеренно: недоступная БД не должна приводить к перезапуску пода.
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(pingCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("postgres unavailable"))
			return
		}
		if err := rdb.Ping(pingCtx).Err(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("redis unavailable"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})

	// ========== HTTP сервер ==========
	srv := &http.Server{
		Addr:         httpAddr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// ========== Outbox Worker (для Analytics/Notification, не относится к саге) ==========
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	outboxWorker := worker.NewOutboxWorker(repo, brokersList, "url-events")
	go outboxWorker.Start(workerCtx)

	// ========== Consumer ответов саги от Billing ==========
	sagaCtx, cancelSaga := context.WithCancel(context.Background())
	go responseConsumer.Start(sagaCtx)

	go func() {
		log.Printf("Server is running on %s...", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Listen error: %v", err)
		}
	}()

	// ========== Graceful Shutdown ==========
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Сначала останавливаем фоновые процессы, затем даём HTTP-серверу 5 секунд
	// на завершение текущих запросов.
	<-quit

	cancelWorker()
	cancelSaga()
	sagaProducer.Close()
	responseConsumer.Close()
	log.Println("Shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server stopped gracefully")
}
