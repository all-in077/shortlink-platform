package grpc

import (
	"auth-service/internal/domain"
	"auth-service/internal/proto/authpb"
	"auth-service/internal/service"
	"context"
	"errors"
)

// AuthGRPCHandler — межсервисный API: gateway проверяет токены на каждом запросе.
type AuthGRPCHandler struct {
	authpb.UnimplementedAuthServiceServer
	authService *service.AuthService
}

// NewAuthGRPCHandler создаёт handler.
func NewAuthGRPCHandler(authService *service.AuthService) *AuthGRPCHandler {
	return &AuthGRPCHandler{authService: authService}
}

// ValidateToken проверяет access-токен. Невалидный или просроченный токен — штатный
// результат (Valid=false, без gRPC-ошибки): ошибка сигнализирует только о сбое самого вызова.
func (h *AuthGRPCHandler) ValidateToken(ctx context.Context, req *authpb.ValidationRequest) (*authpb.ValidationResponse, error) {
	token := req.GetToken()

	uID, err := h.authService.ValidateToken(ctx, token)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidToken) {
			return &authpb.ValidationResponse{Valid: false}, nil
		}
		return nil, err
	}

	return &authpb.ValidationResponse{UserId: uID.String(), Valid: true}, nil
}
