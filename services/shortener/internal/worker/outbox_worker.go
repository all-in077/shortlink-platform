package worker

import (
	"context"
	"log"
	"time"

	"url-shortener/internal/domain"
	"url-shortener/internal/metrics"

	"github.com/segmentio/kafka-go"
)

// OutboxRepository читает и помечает события outbox в одной транзакции.
type OutboxRepository interface {
	ProcessPendingEvents(ctx context.Context, limit int, handle func(events []domain.OutboxEvent) ([]int64, error)) error
}

// OutboxWorker периодически переносит события из outbox в Kafka (transactional outbox):
// публикация не зависит от успеха HTTP-запроса, а событие не теряется при сбое процесса.
type OutboxWorker struct {
	repo         OutboxRepository
	kafkaWriter  *kafka.Writer
	pollInterval time.Duration
}

// NewOutboxWorker создаёт воркер; topic — топик, куда уходят события.
func NewOutboxWorker(repo OutboxRepository, kafkaBrokers []string, topic string) *OutboxWorker {
	// kafka.Writer берет на себя подключение, ретраи и отправку батчей
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBrokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Равномерно распределяет сообщения по партициям Kafka
	}

	return &OutboxWorker{
		repo:         repo,
		kafkaWriter:  writer,
		pollInterval: 2 * time.Second, // Каждые 2 секунды проверяем новые pending-события
	}
}

// Start запускает фоновый процесс опроса БД и отправки сообщений.
func (w *OutboxWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	log.Println("[Outbox Worker] Started successfully...")

	for {
		select {
		case <-ctx.Done():
			// Когда приходит сигнал отмены (например при SIGINT), плавно закрываем сокет Kafka
			log.Println("[Outbox Worker] Shutting down...")
			if err := w.kafkaWriter.Close(); err != nil {
				log.Printf("[Outbox Worker Error] Failed to close Kafka writer: %v", err)
			}
			return

		case <-ticker.C:
			w.processEvents(ctx)
		}
	}
}

// processEvents забирает пачку событий и публикует их в Kafka внутри транзакции репозитория.
// Гарантия at-least-once: дубль возможен, если процесс упадёт после публикации, но до COMMIT.
func (w *OutboxWorker) processEvents(ctx context.Context) {
	err := w.repo.ProcessPendingEvents(ctx, 10, func(events []domain.OutboxEvent) ([]int64, error) {
		metrics.OutboxPendingEvents.Set(float64(len(events)))

		processedIDs := make([]int64, 0, len(events))
		for _, event := range events {
			msg := kafka.Message{
				Key:   []byte(event.EventType),
				Value: event.Payload,
			}

			if err := w.kafkaWriter.WriteMessages(ctx, msg); err != nil {
				log.Printf("[Outbox Worker Error] Failed to send event %d to Kafka: %v", event.ID, err)
				continue // пропускаем этот ID — он не попадёт в processedIDs, останется pending
			}

			processedIDs = append(processedIDs, event.ID)
			log.Printf("[Outbox Worker] Successfully processed event ID %d -> Kafka topic '%s'", event.ID, w.kafkaWriter.Topic)
		}

		return processedIDs, nil
	})

	if err != nil {
		log.Printf("[Outbox Worker Error] ProcessPendingEvents failed: %v", err)
	}
}
