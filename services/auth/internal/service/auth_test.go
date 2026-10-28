package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"auth-service/internal/domain"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// ---------- моки ----------

type mockRepo struct {
	mu     sync.Mutex
	users  map[string]*domain.User
	tokens map[string]*domain.RefreshToken

	revokedTokens   []string
	revokedAllFor   []uuid.UUID
	savedTokens     []string
	createUserCalls int
	getTokenErr     error
	// lostRevokeRace имитирует параллельный запрос, который отозвал токен раньше нас.
	lostRevokeRace bool
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		users:  map[string]*domain.User{},
		tokens: map[string]*domain.RefreshToken{},
	}
}

func (m *mockRepo) CreateUser(ctx context.Context, u *domain.User) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.users[u.Email] = u
	return nil
}

func (m *mockRepo) GetUserByEmail(ctx context.Context, email string) (*domain.User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.users[email]
	if !ok {
		return nil, domain.ErrUserNotFound
	}
	return u, nil
}

func (m *mockRepo) GetUserByID(ctx context.Context, id uuid.UUID) (*domain.User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, u := range m.users {
		if u.ID == id {
			return u, nil
		}
	}
	return nil, domain.ErrUserNotFound
}

func (m *mockRepo) CreateUserWithToken(ctx context.Context, u *domain.User, token string, exp time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createUserCalls++
	if _, exists := m.users[u.Email]; exists {
		return domain.ErrEmailAlreadyExist
	}
	m.users[u.Email] = u
	m.tokens[token] = &domain.RefreshToken{Token: token, UserID: u.ID, ExpiresAt: exp}
	return nil
}

func (m *mockRepo) SaveRefreshToken(ctx context.Context, token string, userID uuid.UUID, exp time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.savedTokens = append(m.savedTokens, token)
	m.tokens[token] = &domain.RefreshToken{Token: token, UserID: userID, ExpiresAt: exp}
	return nil
}

// GetRefreshToken возвращает копию, как настоящая БД: чужие изменения не видны через указатель.
func (m *mockRepo) GetRefreshToken(ctx context.Context, token string) (*domain.RefreshToken, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getTokenErr != nil {
		return nil, m.getTokenErr
	}
	t, ok := m.tokens[token]
	if !ok {
		return nil, domain.ErrTokenNotFound
	}
	cp := *t
	return &cp, nil
}

// RevokeRefreshToken повторяет семантику UPDATE ... WHERE revoked = false:
// true получает только тот вызов, который реально сменил состояние.
func (m *mockRepo) RevokeRefreshToken(ctx context.Context, token string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lostRevokeRace {
		return false, nil
	}
	t, ok := m.tokens[token]
	if !ok || t.Revoked {
		return false, nil
	}
	t.Revoked = true
	m.revokedTokens = append(m.revokedTokens, token)
	return true, nil
}

func (m *mockRepo) RevokeAllUserTokens(ctx context.Context, userID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revokedAllFor = append(m.revokedAllFor, userID)
	for _, t := range m.tokens {
		if t.UserID == userID {
			t.Revoked = true
		}
	}
	return nil
}

type mockTokens struct {
	mu      sync.Mutex
	counter int
}

func (m *mockTokens) GenerateAccessToken(id uuid.UUID) (string, error) {
	return "access-" + id.String(), nil
}

func (m *mockTokens) ValidateAccessToken(token string) (uuid.UUID, error) {
	return uuid.Nil, domain.ErrInvalidToken
}

func (m *mockTokens) GenerateRefreshToken() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counter++
	return "refresh-" + string(rune('a'+m.counter)), nil
}

func newService(repo *mockRepo) *AuthService {
	return NewAuthService(repo, &mockTokens{}, 168*time.Hour)
}

func mustHash(t *testing.T, password string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(h)
}

// ---------- Register ----------

func TestRegister(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(r *mockRepo)
		wantErr error
	}{
		{
			name:    "new user gets tokens",
			prepare: func(r *mockRepo) {},
		},
		{
			name: "duplicate email",
			prepare: func(r *mockRepo) {
				r.users["a@b.com"] = &domain.User{Email: "a@b.com"}
			},
			wantErr: domain.ErrEmailAlreadyExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newMockRepo()
			tt.prepare(repo)
			svc := newService(repo)

			access, refresh, err := svc.Register(context.Background(), "a@b.com", "password123")

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && (access == "" || refresh == "") {
				t.Fatalf("expected both tokens, got %q %q", access, refresh)
			}
			if tt.wantErr != nil && (access != "" || refresh != "") {
				t.Fatalf("expected empty tokens on error, got %q %q", access, refresh)
			}
		})
	}
}

func TestRegisterStoresHashNotPassword(t *testing.T) {
	repo := newMockRepo()
	svc := newService(repo)

	if _, _, err := svc.Register(context.Background(), "a@b.com", "password123"); err != nil {
		t.Fatal(err)
	}

	u := repo.users["a@b.com"]
	if u.PasswordHash == "password123" {
		t.Fatal("password stored in plain text")
	}
	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte("password123")) != nil {
		t.Fatal("stored hash does not match the password")
	}
}

// ---------- Login ----------

func TestLogin(t *testing.T) {
	tests := []struct {
		name     string
		email    string
		password string
		wantErr  error
	}{
		{"success", "a@b.com", "password123", nil},
		{"wrong password", "a@b.com", "wrong-password", domain.ErrWrongPassword},
		// пользователь не найден маскируется под неверный пароль,
		// чтобы нельзя было перебором узнать, какие email зарегистрированы
		{"unknown user looks like wrong password", "nobody@b.com", "password123", domain.ErrWrongPassword},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newMockRepo()
			repo.users["a@b.com"] = &domain.User{
				ID:           uuid.New(),
				Email:        "a@b.com",
				PasswordHash: mustHash(t, "password123"),
			}
			svc := newService(repo)

			access, refresh, err := svc.Login(context.Background(), tt.email, tt.password)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil {
				if access == "" || refresh == "" {
					t.Fatal("expected tokens on success")
				}
				if len(repo.savedTokens) != 1 {
					t.Fatalf("refresh token must be saved once, got %d", len(repo.savedTokens))
				}
			}
		})
	}
}

func TestLoginInfrastructureErrorIsNotMaskedAsWrongPassword(t *testing.T) {
	// сбой БД не должен выглядеть как "неверный пароль"
	repo := &failingRepo{mockRepo: newMockRepo()}
	svc := NewAuthService(repo, &mockTokens{}, time.Hour)

	_, _, err := svc.Login(context.Background(), "a@b.com", "password123")

	if err == nil || errors.Is(err, domain.ErrWrongPassword) {
		t.Fatalf("expected infrastructure error, got %v", err)
	}
}

type failingRepo struct{ *mockRepo }

func (f *failingRepo) GetUserByEmail(ctx context.Context, email string) (*domain.User, error) {
	return nil, errors.New("db is down")
}

// ---------- Refresh ----------

func TestRefresh(t *testing.T) {
	userID := uuid.New()

	tests := []struct {
		name    string
		token   *domain.RefreshToken // nil: токена нет в БД
		wantErr error
	}{
		{
			name:  "valid token is rotated",
			token: &domain.RefreshToken{Token: "old", UserID: userID, ExpiresAt: time.Now().Add(time.Hour)},
		},
		{
			name:    "unknown token",
			token:   nil,
			wantErr: domain.ErrInvalidToken,
		},
		{
			name:    "expired token",
			token:   &domain.RefreshToken{Token: "old", UserID: userID, ExpiresAt: time.Now().Add(-time.Hour)},
			wantErr: domain.ErrInvalidToken,
		},
		{
			name:    "revoked token",
			token:   &domain.RefreshToken{Token: "old", UserID: userID, ExpiresAt: time.Now().Add(time.Hour), Revoked: true},
			wantErr: domain.ErrInvalidToken,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newMockRepo()
			if tt.token != nil {
				repo.tokens["old"] = tt.token
			}
			svc := newService(repo)

			access, refresh, err := svc.Refresh(context.Background(), "old")

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				if access != "" || refresh != "" {
					t.Fatal("no tokens must be issued on error")
				}
				return
			}
			// успешная ротация: старый токен отозван, новый сохранён
			if len(repo.revokedTokens) != 1 || repo.revokedTokens[0] != "old" {
				t.Fatalf("old token must be revoked, got %v", repo.revokedTokens)
			}
			if len(repo.savedTokens) != 1 || repo.savedTokens[0] != refresh {
				t.Fatalf("new token must be saved, got %v", repo.savedTokens)
			}
			if refresh == "old" {
				t.Fatal("refresh token must change on rotation")
			}
		})
	}
}

func TestRefreshReuseRevokesAllUserTokens(t *testing.T) {
	// повторное использование уже отозванного токена считается кражей:
	// отзываем все токены пользователя
	userID := uuid.New()
	repo := newMockRepo()
	repo.tokens["stolen"] = &domain.RefreshToken{
		Token: "stolen", UserID: userID, ExpiresAt: time.Now().Add(time.Hour), Revoked: true,
	}
	repo.tokens["other-session"] = &domain.RefreshToken{
		Token: "other-session", UserID: userID, ExpiresAt: time.Now().Add(time.Hour),
	}
	svc := newService(repo)

	_, _, err := svc.Refresh(context.Background(), "stolen")

	if !errors.Is(err, domain.ErrInvalidToken) {
		t.Fatalf("err = %v, want ErrInvalidToken", err)
	}
	if len(repo.revokedAllFor) != 1 || repo.revokedAllFor[0] != userID {
		t.Fatalf("RevokeAllUserTokens must be called for the user, got %v", repo.revokedAllFor)
	}
	if !repo.tokens["other-session"].Revoked {
		t.Fatal("other sessions of the user must be revoked")
	}
}

func TestRefreshExpiredTokenDoesNotRevokeAll(t *testing.T) {
	// просроченный токен это не кража, чужие сессии не трогаем
	repo := newMockRepo()
	repo.tokens["old"] = &domain.RefreshToken{
		Token: "old", UserID: uuid.New(), ExpiresAt: time.Now().Add(-time.Hour),
	}
	svc := newService(repo)

	_, _, _ = svc.Refresh(context.Background(), "old")

	if len(repo.revokedAllFor) != 0 {
		t.Fatalf("expired token must not trigger revoke-all, got %v", repo.revokedAllFor)
	}
}

// ---------- гонка при параллельном Refresh ----------

func TestRefreshLostRevokeRaceIsTreatedAsReuse(t *testing.T) {
	// между чтением токена и его отзывом параллельный запрос успел отозвать токен:
	// мы проиграли гонку, токены не выдаём и закрываем все сессии
	userID := uuid.New()
	repo := newMockRepo()
	repo.tokens["old"] = &domain.RefreshToken{Token: "old", UserID: userID, ExpiresAt: time.Now().Add(time.Hour)}
	repo.lostRevokeRace = true
	svc := newService(repo)

	access, refresh, err := svc.Refresh(context.Background(), "old")

	if !errors.Is(err, domain.ErrInvalidToken) {
		t.Fatalf("err = %v, want ErrInvalidToken", err)
	}
	if access != "" || refresh != "" {
		t.Fatal("no tokens must be issued when the race is lost")
	}
	if len(repo.savedTokens) != 0 {
		t.Fatalf("no new refresh token may be saved, got %v", repo.savedTokens)
	}
	if len(repo.revokedAllFor) != 1 || repo.revokedAllFor[0] != userID {
		t.Fatalf("all sessions must be revoked, got %v", repo.revokedAllFor)
	}
}

func TestRefreshConcurrentUseOnlyOneSucceeds(t *testing.T) {
	// два одновременных запроса с одним токеном: пару токенов получит ровно один
	repo := newMockRepo()
	repo.tokens["old"] = &domain.RefreshToken{Token: "old", UserID: uuid.New(), ExpiresAt: time.Now().Add(time.Hour)}
	svc := newService(repo)

	const workers = 8
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := svc.Refresh(context.Background(), "old")
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	succeeded := 0
	for err := range results {
		if err == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("exactly one refresh must succeed, got %d", succeeded)
	}
}
