package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"billing-service/internal/domain"
	"billing-service/internal/service"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// ConfirmationConsumer читает подтверждения от shortener (quota-confirmations)
// и помечает брони подтверждёнными, чтобы sweeper их не откатывал.
type ConfirmationConsumer struct {
	reader  *kafka.Reader
	billing *service.BillingService
}

// NewConfirmationConsumer создаёт consumer подтверждений.
func NewConfirmationConsumer(brokers []string, groupID string, billing *service.BillingService) *ConfirmationConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   "quota-confirmations",
		GroupID: groupID,
	})
	return &ConfirmationConsumer{reader: reader, billing: billing}
}

// Start читает подтверждения до отмены контекста. Потерянное подтверждение означало бы
// ложную компенсацию, поэтому при сбое обработка повторяется на месте.
// Повтор безопасен: UPDATE confirmed = true идемпотентен.
func (c *ConfirmationConsumer) Start(ctx context.Context) {
	log.Println("Billing confirmation consumer started...")
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("confirmation consumer: fetch error: %v", err)
			continue
		}

		var event domain.ConfirmationEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("confirmation consumer: failed to unmarshal: %v", err)
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		if !c.handleWithRetry(ctx, event) {
			return
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("confirmation consumer: commit error: %v", err)
		}
	}
}

// handleWithRetry повторяет обработку раз в секунду. Возвращает false, если контекст отменён.
func (c *ConfirmationConsumer) handleWithRetry(ctx context.Context, event domain.ConfirmationEvent) bool {
	for {
		err := c.handleConfirmation(ctx, event)
		if err == nil {
			return true
		}
		log.Printf("confirmation consumer: handle failed, retrying: %v", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Second):
		}
	}
}

func (c *ConfirmationConsumer) handleConfirmation(ctx context.Context, event domain.ConfirmationEvent) error {
	requestID, err := uuid.Parse(event.RequestID)
	if err != nil {
		log.Printf("confirmation consumer: invalid request_id, skipping: %v", err)
		return nil
	}
	return c.billing.ConfirmReservation(ctx, requestID)
}

// Close закрывает reader.
func (c *ConfirmationConsumer) Close() error {
	return c.reader.Close()
}
