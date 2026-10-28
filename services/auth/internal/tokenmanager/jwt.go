package tokenmanager

import (
	"auth-service/internal/domain"
	"crypto/rand"
	"encoding/base64"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Claims — содержимое access-токена: идентификатор пользователя и стандартные поля срока действия.
type Claims struct {
	UserID string `json:"user_id"`
	jwt.RegisteredClaims
}

// JWTManager выпускает и проверяет JWT, подписанные HS256 общим секретом.
type JWTManager struct {
	secretKey []byte
	accessTTL time.Duration
}

func NewJWTManager(secretKey string, accessTTL time.Duration) *JWTManager {
	return &JWTManager{secretKey: []byte(secretKey), accessTTL: accessTTL}
}

// GenerateAccessToken выдаёт короткоживущий JWT. Токен не шифруется, а только подписывается:
// секретные данные в claims класть нельзя.
func (m *JWTManager) GenerateAccessToken(userID uuid.UUID) (string, error) {
	claims := Claims{
		UserID: userID.String(),
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(m.accessTTL)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signedString, err := token.SignedString(m.secretKey)

	return signedString, err

}

// ValidateAccessToken проверяет подпись, алгоритм (только HS256, защита от подмены алгоритма) и срок.
// Любая ошибка сводится к ErrInvalidToken.
func (m *JWTManager) ValidateAccessToken(token string) (uuid.UUID, error) {
	parsedToken, err := jwt.ParseWithClaims(token, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		return m.secretKey, nil
	}, jwt.WithValidMethods([]string{"HS256"}))

	if err != nil {
		// сюда попадут и "подпись не сошлась", и "токен истёк" — библиотека сама разбирается
		return uuid.Nil, domain.ErrInvalidToken
	}

	claims, ok := parsedToken.Claims.(*Claims)
	if !ok || !parsedToken.Valid {
		return uuid.Nil, domain.ErrInvalidToken
	}

	userID, err := uuid.Parse(claims.UserID)
	if err != nil {
		return uuid.Nil, domain.ErrInvalidToken
	}
	return userID, nil
}

// GenerateRefreshToken возвращает 256 бит случайных данных из crypto/rand в base64.
// Это не JWT: смысла в токене нет, он служит ключом записи в БД.
func (m *JWTManager) GenerateRefreshToken() (string, error) {
	b := make([]byte, 32) // 32 байта = 256 бит энтропии
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}
