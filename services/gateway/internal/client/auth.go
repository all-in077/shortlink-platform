package client

import (
	"context"
	"fmt"
	"gateway-service/internal/proto/authpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// AuthClient — gRPC-клиент сервиса auth.
type AuthClient struct {
	client authpb.AuthServiceClient // сгенерированный stub
	conn   *grpc.ClientConn         // трубка (нужна для закрытия)
}

// NewAuthClient создаёт клиент. Соединение устанавливается лениво, при первом вызове;
// TLS не используется, потому что трафик идёт внутри закрытой сети.
func NewAuthClient(addr string) (*AuthClient, error) {
	// grpc.NewClient — современный способ создать соединение.
	// insecure.NewCredentials() — "без TLS". Внутри Docker-сети пока ок,
	// в проде заменим на настоящие сертификаты.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection: %w", err)
	}

	// Создаём stub. Это не открывает новое TCP-соединение,
	// а просто "привязывает" методы к conn.
	// Все будущие RPC пойдут через этот conn.
	stub := authpb.NewAuthServiceClient(conn)

	return &AuthClient{
		client: stub,
		conn:   conn,
	}, nil
}

// Close закрывает gRPC-соединение.
func (c *AuthClient) Close() error {
	return c.conn.Close()
}

// ValidateToken спрашивает у auth, валиден ли токен. Невалидный токен — штатный результат
// (valid=false без ошибки), ошибка означает сбой самого вызова.
func (c *AuthClient) ValidateToken(ctx context.Context, token string) (string, bool, error) {

	resp, err := c.client.ValidateToken(ctx, &authpb.ValidationRequest{Token: token})
	if err != nil {
		return "", false, fmt.Errorf("auth service gRPC error: %w", err)
	}
	return resp.GetUserId(), resp.GetValid(), nil
}
