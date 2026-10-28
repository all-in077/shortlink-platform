package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"
	"url-shortener/internal/pending"
	"url-shortener/internal/saga"
	"url-shortener/internal/service"

	redisRepo "url-shortener/internal/repository/redis"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// CreateURLRequest — тело запроса POST /api/v1/shorten.
type CreateURLRequest struct {
	URL string `json:"url"`
}

// CreateURLResponse — успешный ответ: полная короткая ссылка и её код.
type CreateURLResponse struct {
	ShortURL  string `json:"short_url"`
	ShortCode string `json:"short_code"`
}

// Handler — HTTP-слой shortener. Создание ссылки проходит сагу резервирования квоты
// через Kafka, поэтому хендлеру нужны producer, реестр ожиданий и хранилище идемпотентности.
type Handler struct {
	service      *service.URLService
	baseURL      string
	sagaProducer *saga.Producer
	registry     *pending.Registry
	sagaTimeout  time.Duration
	idem         *redisRepo.IdempotencyStore
}

// NewHandler создаёт Handler. sagaTimeout — сколько ждать ответ Billing.
func NewHandler(svc *service.URLService, baseURL string, sagaProducer *saga.Producer, registry *pending.Registry, sagaTimeout time.Duration, idem *redisRepo.IdempotencyStore) *Handler {
	return &Handler{
		service:      svc,
		baseURL:      baseURL,
		sagaProducer: sagaProducer,
		registry:     registry,
		sagaTimeout:  sagaTimeout,
		idem:         idem,
	}
}

// CreateShortURL обрабатывает POST /api/v1/shorten.
//
// Порядок: идемпотентность -> резерв квоты у Billing через Kafka -> создание ссылки ->
// подтверждение Billing -> ответ клиенту. X-User-ID выставляет gateway после проверки JWT,
// поэтому shortener не должен быть доступен снаружи напрямую.
// Ответы: 201 успех, 429 квота исчерпана, 503 Billing не ответил за sagaTimeout,
// 409 запрос с тем же Idempotency-Key ещё выполняется.
func (h *Handler) CreateShortURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "unavaliable method", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var reqBody CreateURLRequest
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	if reqBody.URL == "" {
		http.Error(w, "url field is required", http.StatusBadRequest)
		return
	}

	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		http.Error(w, "missing user id", http.StatusUnauthorized)
		return
	}

	// Idempotency-Key необязателен. Ключ привязан к пользователю, чтобы один и тот же
	// ключ у разных пользователей не пересекался.
	var fullKey string
	succeeded := false
	if idemKey := r.Header.Get("Idempotency-Key"); idemKey != "" {
		fullKey = userID + ":" + idemKey
		acquired, existing, err := h.idem.Acquire(r.Context(), fullKey, 60*time.Second)
		if err != nil {
			log.Printf("[ERROR] idempotency acquire: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		if !acquired {
			if redisRepo.IsProcessing(existing) {
				http.Error(w, "request is being processed", http.StatusConflict)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotent-Replay", "true")
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(existing))
			return
		}
		defer func() {
			if !succeeded {
				_ = h.idem.Release(context.Background(), fullKey)
			}
		}()
	}

	// Сначала регистрируем ожидание и только потом публикуем запрос: иначе быстрый
	// ответ Billing мог бы прийти раньше, чем появится канал в реестре.
	requestID := uuid.New().String()
	resultCh := h.registry.Register(requestID)

	// Порядок важен: ссылка создана -> подтверждение Billing -> ответ клиенту.
	// Если упасть между шагами, клиент повторит запрос, а у Billing подтверждение уже есть.
	// Если подтверждение не дошло, sweeper вернёт квоту, хотя ссылка создана (известное ограничение).
	if err := h.sagaProducer.PublishRequest(r.Context(), saga.QuotaRequestEvent{
		RequestID: requestID,
		UserID:    userID,
	}); err != nil {
		h.registry.Cancel(requestID)
		log.Printf("[ERROR] failed to publish quota request: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	var quotaResult pending.QuotaResult
	select {
	case quotaResult = <-resultCh:
	case <-time.After(h.sagaTimeout):
		h.registry.Cancel(requestID)
		http.Error(w, "billing service timeout", http.StatusServiceUnavailable)
		return
	}

	if !quotaResult.Approved {
		http.Error(w, quotaResult.Reason, http.StatusTooManyRequests)
		return
	}

	shortURL, err := h.service.CreateShortURL(r.Context(), reqBody.URL)
	if err != nil {
		log.Printf("[ERROR] CreateShortURL: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if err := h.sagaProducer.PublishConfirmation(r.Context(), saga.ConfirmationEvent{
		RequestID: requestID,
	}); err != nil {
		log.Printf("[ERROR] failed to publish confirmation: %v", err)
	}

	resp := CreateURLResponse{
		ShortURL:  fmt.Sprintf("%s/%s", h.baseURL, shortURL.ShortCode),
		ShortCode: shortURL.ShortCode,
	}
	body, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}

	if fullKey != "" {
		if err := h.idem.Save(r.Context(), fullKey, string(body), 24*time.Hour); err == nil {
			succeeded = true
		} else {
			log.Printf("[ERROR] idempotency save: %v", err)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(body)
}

// Redirect обрабатывает GET /r/{code}: перенаправляет на исходный URL и учитывает переход.
// Сбой учёта не мешает редиректу: потерять одну запись в аналитике лучше, чем сломать ссылку.
func (h *Handler) Redirect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "unavaliable method", http.StatusMethodNotAllowed)
		return
	}

	code := chi.URLParam(r, "code")
	if code == "" {
		http.Error(w, "invalid short code", http.StatusBadRequest)
		return
	}

	url, err := h.service.GetByCode(r.Context(), code)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "url not found", http.StatusNotFound)
			return
		}
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if err := h.service.RecordVisit(r.Context(), code, url.LongURL); err != nil {
		log.Printf("[ERROR] RecordVisit: %v", err)
		// не блокируем редирект из-за сбоя учёта посещений
	}

	http.Redirect(w, r, url.LongURL, http.StatusFound)
}
