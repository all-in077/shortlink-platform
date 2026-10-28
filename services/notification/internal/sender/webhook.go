package sender

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// WebhookPayload — тело вебхука.
type WebhookPayload struct {
	EventType  string `json:"event_type"`
	ShortKey   string `json:"short_key"`
	VisitCount int64  `json:"visit_count"`
	Message    string `json:"message"`
}

// Sender отправляет вебхуки на фиксированный адрес.
type Sender struct {
	client     *http.Client
	webhookURL string
}

// NewSender создаёт Sender. Общий таймаут клиента страхует от зависших соединений.
func NewSender(webhookURL string) *Sender {
	client := http.Client{Timeout: 5 * time.Second}
	return &Sender{client: &client, webhookURL: webhookURL}
}

// Send отправляет вебхук. Любой ответ вне 2xx считается ошибкой, чтобы вызывающий код повторил попытку.
func (s *Sender) Send(ctx context.Context, payload WebhookPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal webhook payload: %w", err)
	}

	// Таймаут накладывается поверх входного контекста: отмена снаружи сохраняется.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status: %d", resp.StatusCode)
	}

	log.Printf("webhook sent, status: %d", resp.StatusCode)
	return nil
}
