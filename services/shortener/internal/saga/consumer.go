package saga

import (
	"context"
	"encoding/json"
	"log"

	"url-shortener/internal/pending"

	"github.com/segmentio/kafka-go"
)

// QuotaResponseEvent — ответ Billing на запрос резервирования квоты.
type QuotaResponseEvent struct {
	RequestID string `json:"request_id"`
	Approved  bool   `json:"approved"`
	Reason    string `json:"reason,omitempty"`
}

// ResponseConsumer читает ответы Billing и передаёт их ждущим хендлерам через Registry.
type ResponseConsumer struct {
	reader   *kafka.Reader
	registry *pending.Registry
}

// NewResponseConsumer создаёт consumer. groupID должен быть уникален для каждого пода:
// каждый под обязан получать все ответы и сам решать, чей это request_id
// (иначе внутри одной группы ответ достанется случайному поду и запрос закончится таймаутом).
func NewResponseConsumer(brokers []string, groupID string, registry *pending.Registry) *ResponseConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       "quota-responses",
		GroupID:     groupID,
		StartOffset: kafka.LastOffset, // LastOffset: у новой группы нет сохранённых офсетов, а старые ответы никому не нужны.
	})
	return &ResponseConsumer{reader: reader, registry: registry}
}

// Start читает ответы до отмены контекста. Обработка не может завершиться ошибкой
// (это только поиск в памяти), поэтому офсеты коммитятся сразу.
func (c *ResponseConsumer) Start(ctx context.Context) {
	log.Println("Shortener quota-response consumer started...")
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("response consumer: fetch error: %v", err)
			continue
		}

		var event QuotaResponseEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("response consumer: failed to unmarshal: %v", err)
			c.reader.CommitMessages(ctx, msg)
			continue
		}

		// Чужие ответы (request_id другого пода) Resolve молча пропускает: это штатно.
		c.registry.Resolve(event.RequestID, pending.QuotaResult{
			Approved: event.Approved,
			Reason:   event.Reason,
		})

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("response consumer: commit error: %v", err)
		}
	}
}

func (c *ResponseConsumer) Close() error {
	return c.reader.Close()
}
