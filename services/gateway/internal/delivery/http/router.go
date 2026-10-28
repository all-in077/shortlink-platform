package http

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"

	"gateway-service/internal/client"
	"gateway-service/internal/middleware"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
)

// Limiter ограничивает частоту запросов; реализация подставляется снаружи (в проде Redis).
type Limiter interface {
	Limit(http.Handler) http.Handler
}

// NewRouter собирает маршруты: /health, /r/{code} и /api/v1/auth/* публичные,
// всё остальное требует JWT. Лимитер стоит первым: дешёвая проверка отсекает флуд
// до обращения к auth. Вход и регистрация идут через gateway, чтобы лимитер
// защищал их от перебора паролей.
func NewRouter(authClient *client.AuthClient, shortenerURL, authHTTPURL string, rateLimiter Limiter) http.Handler {
	shortenerTarget, err := url.Parse(shortenerURL)
	if err != nil {
		log.Fatalf("invalid shortener URL: %v", err)
	}
	authTarget, err := url.Parse(authHTTPURL)
	if err != nil {
		log.Fatalf("invalid auth URL: %v", err)
	}

	proxy := httputil.NewSingleHostReverseProxy(shortenerTarget)
	authProxy := httputil.NewSingleHostReverseProxy(authTarget)

	r := chi.NewRouter()

	r.Use(chimiddleware.Logger)
	r.Use(chimiddleware.Recoverer)
	// Доверяем только X-User-ID, выставленному после проверки токена: входящий затираем.
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Header.Del("X-User-ID")
			next.ServeHTTP(w, r)
		})
	})
	r.Use(rateLimiter.Limit)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// Короткие ссылки открывают анонимные пользователи, поэтому без авторизации.
	r.Get("/r/{code}", proxy.ServeHTTP)

	// Вход, регистрация и обновление токена: токена у клиента ещё нет.
	r.Post("/api/v1/auth/register", authProxy.ServeHTTP)
	r.Post("/api/v1/auth/login", authProxy.ServeHTTP)
	r.Post("/api/v1/auth/refresh", authProxy.ServeHTTP)

	r.With(middleware.AuthMiddleware(authClient)).HandleFunc("/*", proxy.ServeHTTP)

	return r
}
