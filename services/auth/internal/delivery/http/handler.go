package http

import (
	"auth-service/internal/domain"
	"auth-service/internal/service"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// AuthHTTPHandler — публичный REST API аутентификации.
type AuthHTTPHandler struct {
	authService *service.AuthService
}

// NewAuthHTTPHandler создаёт handler.
func NewAuthHTTPHandler(authService *service.AuthService) *AuthHTTPHandler {
	return &AuthHTTPHandler{authService: authService}
}

type RegisterRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type RegisterResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type LoginResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type RefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type RefreshResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}

// RegisterUser создаёт пользователя и сразу выдаёт пару токенов. 201 успех, 400 некорректные
// данные, 409 email занят. Пароль длиннее 72 байт отклоняется: bcrypt принимает не больше.
func (h *AuthHTTPHandler) RegisterUser(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if len(req.Email) == 0 || !strings.Contains(req.Email, "@") || len(req.Password) < 8 || len(req.Password) > 72 {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return

	}

	accessToken, refreshToken, err := h.authService.Register(r.Context(), req.Email, req.Password)

	if err != nil {
		if errors.Is(err, domain.ErrEmailAlreadyExist) {
			respondError(w, http.StatusConflict, "email already exists")
			return
		} else {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	resp := RegisterResponse{AccessToken: accessToken, RefreshToken: refreshToken}
	json.NewEncoder(w).Encode(resp)

}

// LoginUser проверяет email и пароль. Любая ошибка учётных данных даёт один ответ 401,
// чтобы по ответу нельзя было узнать, зарегистрирован ли email.
func (h *AuthHTTPHandler) LoginUser(w http.ResponseWriter, r *http.Request) {
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Email == "" || req.Password == "" {
		respondError(w, http.StatusBadRequest, "email and password are required")
		return
	}

	accessToken, refreshToken, err := h.authService.Login(r.Context(), req.Email, req.Password)
	if err != nil {
		if errors.Is(err, domain.ErrWrongPassword) {
			respondError(w, http.StatusUnauthorized, "invalid credentials")
			return
		}
		respondError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	respondJSON(w, http.StatusOK, LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	})
}

// RefreshToken обменивает refresh-токен на новую пару. Каждый токен одноразовый (ротация).
func (h *AuthHTTPHandler) RefreshToken(w http.ResponseWriter, r *http.Request) {
	var req RefreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.RefreshToken == "" {
		respondError(w, http.StatusBadRequest, "refresh token is required")
		return
	}

	accessToken, refreshToken, err := h.authService.Refresh(r.Context(), req.RefreshToken)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidToken) || errors.Is(err, domain.ErrTokenNotFound) {
			respondError(w, http.StatusUnauthorized, "invalid or expired refresh token")
			return
		}
		respondError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	respondJSON(w, http.StatusOK, RefreshResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	})
}
