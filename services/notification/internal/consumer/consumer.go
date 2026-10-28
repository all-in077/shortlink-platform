package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"notification-service/internal/domain"

	"github.com/segmentio/kafka-go"
)

// maxHandleAttempts — сколько раз пробуем обработать событие, прежде чем пропустить его.
// Бесконечный повтор заблокировал бы партицию из-за одного недоступного вебхука.
const maxHandleAttempts = 5

// Consumer читает события из Kafka и передаёт их обработчику.
// Сам он ничего не знает о бизнес-логике уведомлений: её приносит handler.
type Consumer struct {
	reader  *kafka.Reader
	handler func(context.Context, domain.URLEvent) error
}

// NewConsumer создаёт consumer в группе groupID. Новая группа читает топик с начала:
// переход, случившийся до первого запуска, тоже должен попасть в счётчик порога.
func NewConsumer(brokers []string, groupID, topic string, handler func(context.Context, domain.URLEvent) error) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		// MinBytes=1: иначе брокер удерживает запрос, пока не накопится объём
		// или не истечёт MaxWait, и уведомления задерживаются при малом трафике.
		MinBytes:    1,
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.FirstOffset,
	})

	return &Consumer{reader: reader, handler: handler}
}

// Start читает события до отмены контекста. Офсет коммитится только после обработки
// (FetchMessage вместо ReadMessage: тот коммитит сразу при чтении, и сбой обработки
// уже нельзя было бы отследить).
func (c *Consumer) Start(ctx context.Context) {
	log.Println("Notification Consumer started...")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // штатная остановка
			}
			log.Printf("read error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if len(msg.Value) == 0 {
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		var event domain.URLEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			// Нечитаемое сообщение не станет валидным от повтора: пропускаем.
			log.Printf("json unmarshal error: %v", err)
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		if !c.handleWithRetry(ctx, event) {
			return // контекст отменён во время повторов, офсет не коммитим
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}

// handleWithRetry повторяет обработку на месте: kafka-go не отдаёт необработанное
// сообщение повторно, а коммит следующего сдвинул бы офсет мимо него.
// Возвращает false, только если контекст отменён. Исчерпав попытки, возвращает true:
// событие пропускается (в проекте нет dead-letter топика).
func (c *Consumer) handleWithRetry(ctx context.Context, event domain.URLEvent) bool {
	for attempt := 1; attempt <= maxHandleAttempts; attempt++ {
		err := c.handler(ctx, event)
		if err == nil {
			return true
		}
		log.Printf("handler error (attempt %d/%d): %v", attempt, maxHandleAttempts, err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Second):
		}
	}
	log.Printf("giving up on event %s %s: dropped", event.EventType, event.ShortKey)
	return true
}

// Stop закрывает reader; блокирующий FetchMessage при этом возвращается с ошибкой.
func (c *Consumer) Stop() error {
	log.Println("Consumer stopping..")
	return c.reader.Close()
}
