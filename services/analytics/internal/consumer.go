package analytics

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Consumer читает события из Kafka и пишет их в ClickHouse пачками (micro-batching):
// одна большая вставка эффективнее множества одиночных, а ClickHouse рассчитан именно на неё.
// Пачка сбрасывается, когда набралось batchSize событий или прошло flushInterval.
type Consumer struct {
	reader        *kafka.Reader
	repo          *Repository
	batchSize     int
	flushInterval time.Duration
}

// NewConsumer создаёт consumer в группе groupID. Пачка: до 100 событий или 2 секунды.
func NewConsumer(brokers []string, topic, groupID string, repo *Repository) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10,
		MaxBytes: 10e6, // 10MB
	})

	return &Consumer{
		reader:        reader,
		repo:          repo,
		batchSize:     100,
		flushInterval: 2 * time.Second,
	}
}

// Start читает события до отмены контекста. Гарантия доставки at-least-once:
// офсеты коммитятся только после успешной записи в ClickHouse, а при сбое записи буфер
// сохраняется и повторяется на следующем тике. Если процесс упадёт между записью и коммитом,
// пачка будет прочитана заново, поэтому в ClickHouse возможны дубли.
func (c *Consumer) Start(ctx context.Context) {
	log.Println("Analytics Consumer started...")

	events := make([]Event, 0, c.batchSize)
	messages := make([]kafka.Message, 0, c.batchSize)

	// Буфер очищается только после успешной записи и коммита офсетов.
	flushAndReset := func(ctx context.Context) {
		if len(events) == 0 {
			return
		}
		if err := c.flush(ctx, events, messages); err != nil {
			return // буфер сохраняется, повтор на следующем тике
		}
		events = events[:0]
		messages = messages[:0]
	}

	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping consumer, flushing remaining messages...")
			// Отдельный контекст: исходный уже отменён, а запись надо довести до конца.
			flushAndReset(context.Background())
			_ = c.reader.Close()
			return
		case <-ticker.C:
			flushAndReset(ctx)
		default:
			if len(events) >= c.batchSize {
				// Буфер полон, а запись не удалась (ClickHouse недоступен):
				// новые сообщения не читаем, чтобы буфер не рос без границ.
				time.Sleep(200 * time.Millisecond)
				flushAndReset(ctx)
				continue
			}

			// Короткий таймаут чтения, чтобы цикл регулярно возвращался к проверке
			// ctx.Done() и тикера. Ошибка по таймауту здесь штатная.
			readCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			msg, err := c.reader.FetchMessage(readCtx)
			cancel()
			if err != nil {
				continue
			}

			var event Event
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				// Нечитаемое событие пропускаем: повтор его не исправит.
				// Офсет уйдёт вперёд вместе с коммитом следующей пачки.
				log.Printf("Failed to unmarshal event: %v", err)
				continue
			}
			events = append(events, event)
			messages = append(messages, msg)

			if len(events) >= c.batchSize {
				flushAndReset(ctx)
			}
		}
	}
}

// flush сначала пишет пачку в ClickHouse и только потом коммитит офсеты:
// при сбое между шагами события доставятся повторно (at-least-once), но не потеряются.
// Обратный порядок дал бы at-most-once: сбой после коммита терял бы события.
func (c *Consumer) flush(ctx context.Context, events []Event, messages []kafka.Message) error {
	if err := c.repo.InsertBatch(ctx, events); err != nil {
		log.Printf("Error inserting batch to ClickHouse: %v", err)
		return err
	}
	if err := c.reader.CommitMessages(ctx, messages...); err != nil {
		log.Printf("Failed to commit messages to Kafka: %v", err)
		return err
	}
	log.Printf("Successfully flushed %d events to ClickHouse", len(events))
	return nil
}
