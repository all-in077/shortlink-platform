package main

import (
	grpcDelivery "auth-service/internal/delivery/grpc"
	httpDelivery "auth-service/internal/delivery/http"
	"auth-service/internal/proto/authpb"
	"auth-service/internal/repository/postgres"
	"auth-service/internal/service"
	"auth-service/internal/tokenmanager"
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

// Config — настройки сервиса из переменных окружения.
type Config struct {
	GRPCPort        string
	HTTPPort        string
	HTTPMetricsPort string
	DatabaseURL     string
	JWTSecret       string
	AccessTTL       time.Duration
	RefreshTTL      time.Duration
}

// Конфигурация читается из env с разумными дефолтами; внешние библиотеки конфигов не нужны.
func loadConfig() Config {
	// helper: берем env или дефолт
	getEnv := func(key, fallback string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return fallback
	}

	// JWT_SECRET — единственное обязательное поле. Без него сервис бессмысленен.
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		log.Fatal("JWT_SECRET is required")
	}

	// Парсим длительности. time.ParseDuration понимает "15m", "168h", "1h30m".
	accessTTL, err := time.ParseDuration(getEnv("JWT_ACCESS_TTL", "15m"))
	if err != nil {
		log.Fatalf("invalid JWT_ACCESS_TTL: %v", err)
	}

	refreshTTL, err := time.ParseDuration(getEnv("JWT_REFRESH_TTL", "168h"))
	if err != nil {
		log.Fatalf("invalid JWT_REFRESH_TTL: %v", err)
	}

	return Config{
		GRPCPort:        getEnv("GRPC_PORT", ":50051"),
		HTTPPort:        getEnv("HTTP_PORT", ":8080"),
		HTTPMetricsPort: getEnv("HTTP_METRICS_PORT", ":9090"),
		DatabaseURL:     getEnv("DATABASE_URL", "postgres://user:pass@localhost:5432/auth?sslmode=disable"),
		JWTSecret:       secret,
		AccessTTL:       accessTTL,
		RefreshTTL:      refreshTTL,
	}
}

// Точка входа auth: HTTP API (регистрация, логин, refresh), gRPC для проверки токена
// другими сервисами и отдельный порт для метрик. Остановка по SIGINT/SIGTERM корректная.
func main() {
	// Контекст для graceful shutdown
	// signal.NotifyContext создает контекст, который отменяется при SIGINT (Ctrl+C)
	// или SIGTERM (Docker/Kubernetes посылает при остановке контейнера).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- 2.2 Загружаем конфиг ---
	cfg := loadConfig()

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to create db pool: %v", err)
	}
	// Ping проверяет, что мы реально можем достучаться до БД,
	// а не просто создали объект пула.
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}
	log.Println("database connected")

	repo := postgres.NewUserRepository(db)

	// 4.2 Token Manager — чистая функциональность без зависимостей от БД.
	tokenManager := tokenmanager.NewJWTManager(cfg.JWTSecret, cfg.AccessTTL)

	// Serviceбизнес-логика. Он не знает ни про HTTP, ни про gRPC.
	// Единственное, что ему нужно — контракты repo и tokenManager.
	authService := service.NewAuthService(repo, tokenManager, cfg.RefreshTTL)

	// Delivery — транспортные адаптеры. Они переводят HTTP/gRPC-запросы
	// в вызовы сервисного слоя и обратно.
	grpcHandler := grpcDelivery.NewAuthGRPCHandler(authService)
	httpHandler := httpDelivery.NewAuthHTTPHandler(authService)

	httpMux := http.NewServeMux()
	httpMux.HandleFunc("POST /api/v1/auth/register", httpHandler.RegisterUser)
	httpMux.HandleFunc("POST /api/v1/auth/login", httpHandler.LoginUser)
	httpMux.HandleFunc("POST /api/v1/auth/refresh", httpHandler.RefreshToken)

	httpMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	httpMux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.Ping(pingCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("database unavailable"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})

	httpServer := &http.Server{
		Addr:    cfg.HTTPPort,
		Handler: httpMux,
	}

	go func() {
		log.Printf("HTTP API server started on %s", cfg.HTTPPort)
		// ListenAndServe блокирует горутину до ошибки или Shutdown.
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP API server error: %v", err)
		}
	}()

	// Это межсервисное API. Только Gateway (или другие внутренние сервисы)
	// будут дёргать ValidateToken по gRPC.
	grpcListener, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("failed to listen gRPC port: %v", err)
	}

	grpcServer := grpc.NewServer()
	// Регистрируем наш хендлер как реализацию AuthServiceServer.
	authpb.RegisterAuthServiceServer(grpcServer, grpcHandler)

	go func() {
		log.Printf("gRPC server started on %s", cfg.GRPCPort)
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	// Отдельный порт для Prometheus. Он не торчит наружу — только во внутренней
	// сети для скрейпинга метрик.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())

	metricsServer := &http.Server{
		Addr:    cfg.HTTPMetricsPort,
		Handler: metricsMux,
	}

	go func() {
		log.Printf("metrics server started on %s", cfg.HTTPMetricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()

	// Блокируем main до сигнала (Ctrl+C или SIGTERM).
	<-ctx.Done()
	log.Println("shutting down gracefully...")

	// Создаем фоновый контекст с таймаутом 5 секунд.
	// Если за 5 секунд серверы не завершат текущие запросы — принудительно убьем.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 8.1 gRPC: GracefulStop перестает принимать новые соединения,
	//     но дожидается завершения текущих RPC.
	grpcServer.GracefulStop()
	log.Println("gRPC server stopped")

	// 8.2 HTTP API: Shutdown закрывает слушатель и ждет активных запросов.
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP API shutdown error: %v", err)
	}
	log.Println("HTTP API server stopped")

	// 8.3 HTTP Metrics: аналогично.
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("metrics server shutdown error: %v", err)
	}
	log.Println("metrics server stopped")

	// 8.4 Закрываем пул соединений с Postgres.
	db.Close()
	log.Println("database pool closed")

	log.Println("servers stopped")
}
