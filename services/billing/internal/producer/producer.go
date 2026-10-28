package producer

import (
	"context"
	"encoding/json"

	"billing-service/internal/domain"

	"github.com/segmentio/kafka-go"
)

// Producer публикует ответы Billing в топик quota-responses.
type Producer struct {
	responsesWriter *kafka.Writer
}

// NewProducer создаёт Producer.
func NewProducer(brokers []string) *Producer {
	return &Producer{
		responsesWriter: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: "quota-responses",
		},
	}
}

// PublishResponse отправляет решение по запросу. Ключ сообщения request_id.
func (p *Producer) PublishResponse(ctx context.Context, event domain.QuotaResponseEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.responsesWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.RequestID),
		Value: data,
	})
}

// Close закрывает writer.
func (p *Producer) Close() error {
	return p.responsesWriter.Close()
}
