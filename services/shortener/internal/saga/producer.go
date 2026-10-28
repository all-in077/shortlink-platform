package saga

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

// QuotaRequestEvent — запрос на резервирование квоты для одного создания ссылки.
type QuotaRequestEvent struct {
	RequestID string `json:"request_id"`
	UserID    string `json:"user_id"`
}

// ConfirmationEvent сообщает Billing, что ссылка по request_id создана.
// Без подтверждения sweeper вернёт квоту.
type ConfirmationEvent struct {
	RequestID string `json:"request_id"`
}

// Producer публикует события саги: запрос квоты и подтверждение.
type Producer struct {
	requestsWriter      *kafka.Writer
	confirmationsWriter *kafka.Writer
}

// NewProducer создаёт Producer с отдельным writer на каждый топик.
func NewProducer(brokers []string) *Producer {
	return &Producer{
		requestsWriter: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: "quota-requests",
		},
		confirmationsWriter: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: "quota-confirmations",
		},
	}
}

// PublishRequest отправляет запрос квоты. Ключ сообщения — request_id.
func (p *Producer) PublishRequest(ctx context.Context, event QuotaRequestEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.requestsWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.RequestID),
		Value: data,
	})
}

// PublishConfirmation отправляет подтверждение создания ссылки.
func (p *Producer) PublishConfirmation(ctx context.Context, event ConfirmationEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.confirmationsWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.RequestID),
		Value: data,
	})
}

func (p *Producer) Close() error {
	if err := p.requestsWriter.Close(); err != nil {
		return err
	}
	return p.confirmationsWriter.Close()
}
