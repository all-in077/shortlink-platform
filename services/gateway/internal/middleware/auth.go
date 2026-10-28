package middleware

import (
	"context"
	"encoding/json"
	"gateway-service/internal/client"
	"net/http"
	"strings"
)

// contextKey — собственный тип ключа, чтобы не пересекаться с ключами других пакетов.
type contextKey string

const userIDContextKey contextKey = "userID"

func respondError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// AuthMiddleware проверяет Bearer-токен через auth и передаёт идентификатор пользователя
// дальше в заголовке X-User-ID. При сбое auth запрос отклоняется (fail-closed).
func AuthMiddleware(authClient *client.AuthClient) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			// логика проверки
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
				respondError(w, http.StatusUnauthorized, "unauthorized")
				return
			}

			token := strings.TrimPrefix(authHeader, "Bearer ")

			// Проверяем через gRPC
			userID, valid, err := authClient.ValidateToken(r.Context(), token)
			if err != nil || !valid {
				respondError(w, http.StatusUnauthorized, "unauthorized")
				return
			}

			// Дописываем внутренний заголовок для downstream-сервисов
			r.Header.Set("X-User-ID", userID)

			// Прокидываем в контекст UserID
			ctx := context.WithValue(r.Context(), userIDContextKey, userID)
			r = r.WithContext(ctx)

			// Пропускаем запрос дальше (в Shortener)
			next.ServeHTTP(w, r) // пропускаем дальше
		})
	}

}
