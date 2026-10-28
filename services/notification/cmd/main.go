package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"notification-service/internal/consumer"
	"notification-service/internal/sender"
	"notification-service/internal/service"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// getEnv возвращает переменную окружения или значение по умолчанию.
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Точка входа notification: читает события переходов из Kafka и отправляет вебхук,
// когда ссылка набирает NOTIFY_THRESHOLD переходов. Параллельно отдаёт метрики и пробы.
// Остановка по SIGINT/SIGTERM корректная.
func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "kafka:29092"), ",")
	topic := getEnv("KAFKA_TOPIC", "url-events")
	groupID := getEnv("KAFKA_GROUP_ID", "notifications-group")
	httpPort := getEnv("HTTP_PORT", ":8086")

	// WEBHOOK_URL обязателен: без адреса отправлять уведомления некуда (fail fast).
	webhookURL := os.Getenv("WEBHOOK_URL")
	if webhookURL == "" {
		log.Fatal("WEBHOOK_URL is required")
	}
	threshold, err := strconv.ParseInt(getEnv("NOTIFY_THRESHOLD", "1000"), 10, 64)
	if err != nil {
		log.Fatalf("invalid NOTIFY_THRESHOLD: %v", err)
	}

	notifier := service.NewNotifier(sender.NewSender(webhookURL), threshold)
	kafkaConsumer := consumer.NewConsumer(brokers, groupID, topic, notifier.HandleEvent)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go kafkaConsumer.Start(ctx)
	go notifier.StartCleanup(ctx, 10*time.Minute, 24*time.Hour)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	// У сервиса нет собственной БД, поэтому проба готовности пока совпадает с проверкой
	// жизни. Настоящая готовность потребовала бы сигнала от consumer'а о подключении к Kafka.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
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
			log.Printf("http server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down notification service...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := kafkaConsumer.Stop(); err != nil {
		log.Printf("consumer stop error: %v", err)
	}
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("http server shutdown error: %v", err)
	}

	log.Println("notification service stopped")
}
