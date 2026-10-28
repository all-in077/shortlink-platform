package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// Ошибки домена. ErrTokenCollision крайне маловероятна: токен случайный (256 бит)
var (
	ErrEmailAlreadyExist = errors.New("email already exist")
	ErrWrongPassword     = errors.New("wrong password")
	ErrInvalidToken      = errors.New("invalid token")
	ErrUserNotFound      = errors.New("user not found")
	ErrTokenCollision    = errors.New("refresh token collision, try again")
	ErrTokenNotFound     = errors.New("token not found")
)

// User — пользователь. В PasswordHash лежит bcrypt-хеш, сам пароль не хранится.
type User struct {
	ID           uuid.UUID
	Email        string
	PasswordHash string
	CreatedAt    time.Time
}

// RefreshToken — долгоживущий одноразовый токен обновления. Хранится в БД, а не в JWT,
// чтобы его можно было отозвать.
type RefreshToken struct {
	ID int64 // выдаётся Postgres (BIGSERIAL) при INSERT; не uuid,
	// т.к. генерируется всегда централизованно в момент записи, а не заранее в коде
	Token     string
	UserID    uuid.UUID
	ExpiresAt time.Time
	Revoked   bool
	CreatedAt time.Time
}

// New создаёт пользователя. ID — UUIDv7: упорядочен по времени, поэтому индекс по первичному
// ключу не фрагментируется так, как при случайном UUIDv4.
func New(email string, passwordHash string) (*User, error) {
	newId, err := uuid.NewV7()

	if err != nil {
		return nil, err
	}

	return &User{ID: newId, Email: email,
		PasswordHash: passwordHash, CreatedAt: time.Now()}, nil
}
