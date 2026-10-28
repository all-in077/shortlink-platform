package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPRequestsTotal считает общее количество запросов.
	// Labels (метки): method, path, status — позволяют фильтровать метрики в Grafana.
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	// HTTPRequestDuration замеряет задержку (latency) выполнения запросов в секундах.
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets, // Дефолтные "корзины" замера времени (от 5мс до 10с)
		},
		[]string{"method", "path"},
	)

	// OutboxPendingEvents — размер последней забранной воркером пачки (не более limit).
	OutboxPendingEvents = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "outbox_pending_events_count",
			Help: "Current number of pending outbox events in Postgres",
		},
	)
)

// PrometheusMiddleware автоматически собирает latency и статус по каждому HTTP-запросу
func PrometheusMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Оборачиваем ResponseWriter, чтобы узнать код ответа (200, 201, 404, 500 и т.д.)
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

		next.ServeHTTP(ww, r)

		// Очищаем путь от параметров (например, из /r/abc1234 получаем шаблон роута)
		path := chi.RouteContext(r.Context()).RoutePattern()
		if path == "" {
			path = "unknown"
		}

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(ww.Status())

		// Записываем данные в метрики
		HTTPRequestsTotal.WithLabelValues(r.Method, path, status).Inc()
		HTTPRequestDuration.WithLabelValues(r.Method, path).Observe(duration)
	})
}
