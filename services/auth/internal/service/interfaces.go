package service

import (
	"auth-service/internal/domain"
	"context"
	"time"

	"github.com/google/uuid"
)

// UserRepository — хранилище пользователей и refresh-токенов.
type UserRepository interface {
	CreateUser(ctx context.Context, user *domain.User) error
	GetUserByEmail(ctx context.Context, email string) (*domain.User, error)
	GetUserByID(ctx context.Context, id uuid.UUID) (*domain.User, error)
	CreateUserWithToken(ctx context.Context, user *domain.User, token string, expiresAt time.Time) error

	SaveRefreshToken(ctx context.Context, token string, userID uuid.UUID, expiresAt time.Time) error
	GetRefreshToken(ctx context.Context, token string) (*domain.RefreshToken, error)
	// RevokeRefreshToken возвращает true, только если токен отозвал именно этот вызов.
	RevokeRefreshToken(ctx context.Context, token string) (bool, error)
	RevokeAllUserTokens(ctx context.Context, userID uuid.UUID) error
}

// TokenManager — выпуск и проверка токенов; отделён от БД, поэтому формат токенов можно менять.
type TokenManager interface {
	GenerateAccessToken(userID uuid.UUID) (string, error)
	ValidateAccessToken(token string) (uuid.UUID, error)
	GenerateRefreshToken() (string, error)
}
