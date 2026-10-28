package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// EventsProcessedTotal — сколько событий обработано, в разрезе типа события.
	EventsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notification_events_processed_total",
			Help: "Total number of Kafka events processed by notification service",
		},
		[]string{"event_type"},
	)

	// WebhooksSentTotal — успешно отправленные вебхуки.
	WebhooksSentTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "notification_webhooks_sent_total",
			Help: "Total number of webhooks successfully sent",
		},
	)

	// WebhooksFailedTotal — неудачные попытки отправки (каждая попытка повтора считается отдельно).
	WebhooksFailedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "notification_webhooks_failed_total",
			Help: "Total number of webhook send failures",
		},
	)
)
