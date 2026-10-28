package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"billing-service/internal/domain"
	"billing-service/internal/producer"
	"billing-service/internal/service"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// RequestConsumer читает запросы на резервирование квоты (quota-requests)
// и публикует решение Billing в топик quota-responses.
type RequestConsumer struct {
	reader   *kafka.Reader
	billing  *service.BillingService
	producer *producer.Producer
}

// NewRequestConsumer создаёт consumer. groupID общий для всех экземпляров billing:
// каждый запрос должен обработать ровно один экземпляр.
func NewRequestConsumer(brokers []string, groupID string, billing *service.BillingService, prod *producer.Producer) *RequestConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   "quota-requests",
		GroupID: groupID,
	})
	return &RequestConsumer{reader: reader, billing: billing, producer: prod}
}

// Start читает запросы до отмены контекста. Офсет коммитится только после успешной обработки.
// kafka-go не отдаёт необработанное сообщение повторно, а коммит следующего сдвинул бы офсет
// мимо него, поэтому при сбое обработка повторяется на месте. Повтор безопасен:
// ReserveWithRecord идемпотентен по request_id.
func (c *RequestConsumer) Start(ctx context.Context) {
	log.Println("Billing request consumer started...")
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("request consumer: fetch error: %v", err)
			continue
		}

		var event domain.QuotaRequestEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("request consumer: failed to unmarshal: %v", err)
			_ = c.reader.CommitMessages(ctx, msg) // мусор не станет валидным, пропускаем
			continue
		}

		if !c.handleWithRetry(ctx, event) {
			return // контекст отменён во время повторов
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("request consumer: commit error: %v", err)
		}
	}
}

// handleWithRetry повторяет обработку раз в секунду, пока она не удастся.
// Возвращает false, если контекст отменён раньше.
func (c *RequestConsumer) handleWithRetry(ctx context.Context, event domain.QuotaRequestEvent) bool {
	for {
		err := c.handleRequest(ctx, event)
		if err == nil {
			return true
		}
		log.Printf("request consumer: handle failed, retrying: %v", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Second):
		}
	}
}

// handleRequest резервирует квоту и публикует ответ. Невалидные идентификаторы пропускаются
// без повтора, отказ по лимиту является штатным ответом, инфраструктурная ошибка
// возвращается наверх для повтора.
func (c *RequestConsumer) handleRequest(ctx context.Context, event domain.QuotaRequestEvent) error {
	requestID, err := uuid.Parse(event.RequestID)
	if err != nil {
		log.Printf("request consumer: invalid request_id, skipping: %v", err)
		return nil
	}
	userID, err := uuid.Parse(event.UserID)
	if err != nil {
		log.Printf("request consumer: invalid user_id, skipping: %v", err)
		return nil
	}

	resp := domain.QuotaResponseEvent{RequestID: event.RequestID, Approved: true}
	if err := c.billing.ReserveQuota(ctx, requestID, userID); err != nil {
		if !errors.Is(err, domain.ErrQuotaExceeded) {
			return err
		}
		resp.Approved = false
		resp.Reason = "quota_exceeded"
	}

	return c.producer.PublishResponse(ctx, resp)
}

// Close закрывает reader.
func (c *RequestConsumer) Close() error {
	return c.reader.Close()
}
