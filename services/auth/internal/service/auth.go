package service

import (
	"auth-service/internal/domain"
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// AuthService — регистрация, вход и обновление токенов.
type AuthService struct {
	repo         UserRepository
	tokenManager TokenManager
	refreshTTL   time.Duration
}

// NewAuthService создаёт сервис. refreshTTL — срок жизни refresh-токена.
func NewAuthService(repo UserRepository, tokenManager TokenManager, refreshTTL time.Duration) *AuthService {
	return &AuthService{repo: repo, tokenManager: tokenManager, refreshTTL: refreshTTL}
}

// Register хеширует пароль (bcrypt), создаёт пользователя вместе с refresh-токеном
// в одной транзакции и выдаёт пару токенов.
func (s *AuthService) Register(ctx context.Context, email, password string) (accessToken, refreshToken string, err error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", "", err
	}

	u, err := domain.New(email, string(hash))
	if err != nil {
		return "", "", err
	}

	accessToken, err = s.tokenManager.GenerateAccessToken(u.ID)
	if err != nil {
		return "", "", err
	}
	refreshToken, err = s.tokenManager.GenerateRefreshToken()
	if err != nil {
		return "", "", err
	}

	if err := s.repo.CreateUserWithToken(ctx, u, refreshToken, time.Now().Add(s.refreshTTL)); err != nil {
		return "", "", err
	}

	return accessToken, refreshToken, nil
}

// Login проверяет учётные данные. Отсутствие пользователя возвращается как ErrWrongPassword:
// иначе перебором можно узнать, какие email зарегистрированы. Сбой БД маскироваться не должен.
func (s *AuthService) Login(ctx context.Context, email, password string) (accessToken, refreshToken string, err error) {
	u, err := s.repo.GetUserByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, domain.ErrUserNotFound) {
			return "", "", domain.ErrWrongPassword
		}
		return "", "", err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)); err != nil {
		return "", "", domain.ErrWrongPassword
	}

	accessToken, err = s.tokenManager.GenerateAccessToken(u.ID)
	if err != nil {
		return "", "", err
	}
	refreshToken, err = s.tokenManager.GenerateRefreshToken()
	if err != nil {
		return "", "", err
	}

	if err := s.repo.SaveRefreshToken(ctx, refreshToken, u.ID, time.Now().Add(s.refreshTTL)); err != nil {
		return "", "", err
	}
	return accessToken, refreshToken, nil
}

// Refresh меняет refresh-токен на новую пару (ротация): каждый токен одноразовый.
// Повторное использование уже отозванного токена считается возможной кражей,
// поэтому отзываются все токены пользователя.
func (s *AuthService) Refresh(ctx context.Context, oldRefreshToken string) (accessToken, refreshToken string, err error) {
	tk, err := s.repo.GetRefreshToken(ctx, oldRefreshToken)
	if err != nil {
		if errors.Is(err, domain.ErrTokenNotFound) {
			return "", "", domain.ErrInvalidToken
		}
		return "", "", err // сбой БД не должен выглядеть как «неверный токен»
	}

	if tk.Revoked {
		// повторное использование: возможная кража, закрываем все сессии
		_ = s.repo.RevokeAllUserTokens(ctx, tk.UserID)
		return "", "", domain.ErrInvalidToken
	}

	// просроченный токен кражей не считается, чужие сессии не трогаем
	if time.Now().After(tk.ExpiresAt) {
		return "", "", domain.ErrInvalidToken
	}

	// Отзыв условный (UPDATE ... WHERE revoked = false): из двух параллельных запросов
	// с одним токеном выиграет один. Проигравший значит, что токен используют повторно.
	won, err := s.repo.RevokeRefreshToken(ctx, oldRefreshToken)
	if err != nil {
		return "", "", err
	}
	if !won {
		_ = s.repo.RevokeAllUserTokens(ctx, tk.UserID)
		return "", "", domain.ErrInvalidToken
	}

	accessToken, err = s.tokenManager.GenerateAccessToken(tk.UserID)
	if err != nil {
		return "", "", err
	}
	refreshToken, err = s.tokenManager.GenerateRefreshToken()
	if err != nil {
		return "", "", err
	}

	if err := s.repo.SaveRefreshToken(ctx, refreshToken, tk.UserID, time.Now().Add(s.refreshTTL)); err != nil {
		return "", "", err
	}
	return accessToken, refreshToken, nil
}

// ValidateToken проверяет access-токен без обращения к БД: подпись и срок действия.
func (s *AuthService) ValidateToken(ctx context.Context, accessToken string) (uuid.UUID, error) {
	return s.tokenManager.ValidateAccessToken(accessToken)
}
