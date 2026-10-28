package postgres

import (
	"auth-service/internal/domain"
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UserRepository — доступ к пользователям и refresh-токенам в Postgres.
type UserRepository struct {
	db *pgxpool.Pool
}

func NewUserRepository(db *pgxpool.Pool) *UserRepository {
	return &UserRepository{db: db}
}

// CreateUser сохраняет пользователя; занятый email возвращается как ErrEmailAlreadyExist
// (уникальность гарантирует БД, а не проверка в коде: она безопасна при гонке).
func (r *UserRepository) CreateUser(ctx context.Context, user *domain.User) error {
	query := `INSERT INTO users (id, email, password_hash, created_at) VALUES ($1, $2, $3, $4)`

	_, err := r.db.Exec(ctx, query, user.ID, user.Email, user.PasswordHash, user.CreatedAt)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return domain.ErrEmailAlreadyExist
		}
		return err
	}
	return nil

}

func (r *UserRepository) GetUserByEmail(ctx context.Context, email string) (*domain.User, error) {

	query := `SELECT id, email, password_hash, created_at FROM users WHERE email=$1`
	var u domain.User

	err := r.db.QueryRow(ctx, query, email).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrUserNotFound
		}
		return nil, err
	}

	return &u, nil
}

func (r *UserRepository) GetUserByID(ctx context.Context, id uuid.UUID) (*domain.User, error) {
	query := `SELECT id, email, password_hash, created_at FROM users WHERE id=$1`
	var u domain.User

	err := r.db.QueryRow(ctx, query, id).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrUserNotFound
		}
		return nil, err
	}

	return &u, nil
}

// CreateUserWithToken создаёт пользователя и первый refresh-токен в одной транзакции:
// пользователь без токена не смог бы войти.
func (r *UserRepository) CreateUserWithToken(ctx context.Context, user *domain.User, token string, expiresAt time.Time) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // no-op, если уже был Commit

	_, err = tx.Exec(ctx, `INSERT INTO users (id, email, password_hash, created_at) VALUES ($1, $2, $3, $4)`,
		user.ID, user.Email, user.PasswordHash, user.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return domain.ErrEmailAlreadyExist
		}
		return err
	}

	_, err = tx.Exec(ctx, `INSERT INTO refresh_tokens (token, user_id, expires_at) VALUES ($1, $2, $3)`,
		token, user.ID, expiresAt)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// SaveRefreshToken сохраняет токен; коллизия возвращается как ErrTokenCollision.
func (r *UserRepository) SaveRefreshToken(ctx context.Context, token string, userID uuid.UUID, expiresAt time.Time) error {
	query := `INSERT INTO refresh_tokens(token, user_id, expires_at) VALUES ($1, $2, $3)`

	_, err := r.db.Exec(ctx, query, token, userID, expiresAt)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return domain.ErrTokenCollision
		}
		return err
	}
	return nil
}

// GetRefreshToken возвращает токен или ErrTokenNotFound.
func (r *UserRepository) GetRefreshToken(ctx context.Context, token string) (*domain.RefreshToken, error) {
	query := `SELECT id, token, user_id, expires_at, created_at, revoked FROM refresh_tokens WHERE token=$1`
	var tk domain.RefreshToken

	err := r.db.QueryRow(ctx, query, token).Scan(&tk.ID, &tk.Token, &tk.UserID, &tk.ExpiresAt, &tk.CreatedAt, &tk.Revoked)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrTokenNotFound
		}
		return nil, err
	}

	return &tk, nil

}

// RevokeRefreshToken отзывает токен и сообщает, удалось ли это именно этому вызову.
// false означает, что токен уже был отозван (в том числе параллельным запросом).
func (r *UserRepository) RevokeRefreshToken(ctx context.Context, token string) (bool, error) {
	tag, err := r.db.Exec(ctx,
		`UPDATE refresh_tokens SET revoked = true WHERE token = $1 AND revoked = false`, token)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// RevokeAllUserTokens отзывает все токены пользователя (все сессии).
func (r *UserRepository) RevokeAllUserTokens(ctx context.Context, userID uuid.UUID) error {
	query := `UPDATE refresh_tokens SET revoked = true WHERE user_id = $1`

	_, err := r.db.Exec(ctx, query, userID)

	return err

}
