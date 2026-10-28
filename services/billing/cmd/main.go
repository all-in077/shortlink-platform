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

	"billing-service/internal/consumer"
	"billing-service/internal/producer"
	"billing-service/internal/repository/postgres"
	"billing-service/internal/service"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Точка входа billing: два consumer'а саги (запросы и подтверждения), sweeper компенсаций
// и HTTP-сервер для метрик и проб. Остановка по SIGINT/SIGTERM отменяет общий контекст.
func main() {
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@billing-db:5432/billing?sslmode=disable")
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "kafka:29092"), ",")
	requestsGroupID := getEnv("REQUESTS_CONSUMER_GROUP", "billing-requests-group")
	confirmationsGroupID := getEnv("CONFIRMATIONS_CONSUMER_GROUP", "billing-confirmations-group")

	// Sweeper проверяет брони раз в 2 секунды и компенсирует те, что не подтверждены 10 секунд.
	// sweepExpireAfter должен быть заметно больше времени создания ссылки в shortener,
	// иначе подтверждение придёт уже после компенсации.
	httpPort := getEnv("HTTP_PORT", ":8084")
	sweepInterval := 2 * time.Second
	sweepExpireAfter := 10 * time.Second

	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	repo := postgres.NewBillingRepository(db)
	billingService := service.NewBillingService(repo)
	prod := producer.NewProducer(kafkaBrokers)

	requestConsumer := consumer.NewRequestConsumer(kafkaBrokers, requestsGroupID, billingService, prod)
	confirmationConsumer := consumer.NewConfirmationConsumer(kafkaBrokers, confirmationsGroupID, billingService)
	sweeper := service.NewSweeper(repo, sweepInterval, sweepExpireAfter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go requestConsumer.Start(ctx)
	go confirmationConsumer.Start(ctx)
	go sweeper.Start(ctx)

	// healthz: процесс жив (liveness). readyz: доступна БД (readiness).
	// Разделены намеренно: недоступная БД не должна приводить к перезапуску пода.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(pingCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("database unavailable"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})

	httpServer := &http.Server{Addr: httpPort, Handler: mux}

	go func() {
		log.Printf("HTTP server (metrics/health) listening on %s", httpPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutdown signal received, stopping billing service...")

	cancel() // останавливает оба consumer'а и sweeper через ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("http server shutdown error: %v", err)
	}

	log.Println("Billing service stopped gracefully")
}
