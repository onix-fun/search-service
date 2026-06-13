package app

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpchealth "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	searchv1 "github.com/company/search-service/api/search/v1"
	"github.com/company/search-service/internal/backend/meili"
	"github.com/company/search-service/internal/broker/rabbitmq"
	"github.com/company/search-service/internal/config"
	"github.com/company/search-service/internal/enrichment"
	"github.com/company/search-service/internal/grpcserver"
	"github.com/company/search-service/internal/health"
	"github.com/company/search-service/internal/indexer"
)

type Role string

const (
	RoleAPI     Role = "api"
	RoleIndexer Role = "indexer"
	RoleAll     Role = "all"
)

func ParseRole(value string) (Role, error) {
	switch Role(value) {
	case RoleAPI, RoleIndexer, RoleAll:
		return Role(value), nil
	default:
		return "", fmt.Errorf("role must be api, indexer or all")
	}
}

func Run(ctx context.Context, cfg config.Config, role Role, logger *slog.Logger) error {
	runAPI := role == RoleAPI || role == RoleAll
	runIndexer := role == RoleIndexer || role == RoleAll

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
	defer redisClient.Close()
	meiliClient := meili.New(cfg.Meilisearch)
	if err := waitForMeilisearch(ctx, meiliClient, cfg.Meilisearch.TaskTimeout); err != nil {
		return fmt.Errorf("wait for meilisearch: %w", err)
	}
	if err := meiliClient.Migrate(ctx, cfg.Enrichment.SynonymsFile); err != nil {
		return fmt.Errorf("migrate meilisearch index: %w", err)
	}
	processor := enrichment.New(cfg.Enrichment.Transliteration, cfg.Enrichment.Morphology)

	var broker *rabbitmq.Broker
	if runIndexer {
		var err error
		broker, err = rabbitmq.New(cfg.RabbitMQ, cfg.Entities, logger)
		if err != nil {
			return fmt.Errorf("connect to rabbitmq: %w", err)
		}
		defer broker.Close()
	}

	probe := health.NewProbe(meiliClient, redisClient, broker, runAPI, runIndexer)

	var listener net.Listener
	var err error
	if runAPI {
		listener, err = net.Listen("tcp", cfg.Service.GRPCAddr)
		if err != nil {
			return fmt.Errorf("listen gRPC: %w", err)
		}
		defer listener.Close()
	}

	httpServer := &http.Server{Addr: cfg.Service.HTTPAddr, Handler: health.Handler(logger, probe), ReadHeaderTimeout: 5 * time.Second}
	errs := make(chan error, 3)
	go func() {
		logger.Info("health server started", "addr", cfg.Service.HTTPAddr)
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- fmt.Errorf("serve health HTTP: %w", err)
		}
	}()

	var grpcServer *grpc.Server
	if runAPI {
		grpcServer = grpc.NewServer(grpc.UnaryInterceptor(requireInternalAuth(cfg.Service.InternalAuthSecret)))
		searchv1.RegisterSearchServiceServer(grpcServer, grpcserver.New(meiliClient, processor, cfg.Search.DefaultLimit, cfg.Search.MaxLimit))
		grpcHealth := grpchealth.NewServer()
		grpcHealth.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		healthpb.RegisterHealthServer(grpcServer, grpcHealth)
		go monitorGRPCHealth(ctx, probe, grpcHealth)
		go func() {
			logger.Info("gRPC server started", "addr", cfg.Service.GRPCAddr)
			if err := grpcServer.Serve(listener); err != nil {
				errs <- fmt.Errorf("serve gRPC: %w", err)
			}
		}()
	}

	if runIndexer {
		worker := indexer.NewWorker(cfg, logger, meiliClient, processor, broker, redisClient)
		go func() {
			if err := worker.Run(ctx); err != nil {
				errs <- fmt.Errorf("run indexer: %w", err)
			}
		}()
	}

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errs:
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if grpcServer != nil {
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-shutdownCtx.Done():
			grpcServer.Stop()
		}
	}
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("health server shutdown failed", "error", err)
	}
	return runErr
}

func requireInternalAuth(secret string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == "/grpc.health.v1.Health/Check" || info.FullMethod == "/grpc.health.v1.Health/Watch" {
			return handler(ctx, req)
		}
		values := metadata.ValueFromIncomingContext(ctx, "x-internal-auth")
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(secret)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid internal service credentials")
		}
		return handler(ctx, req)
	}
}

func monitorGRPCHealth(ctx context.Context, probe *health.Probe, server *grpchealth.Server) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		status := healthpb.HealthCheckResponse_SERVING
		if err := probe.Ready(ctx); err != nil {
			status = healthpb.HealthCheckResponse_NOT_SERVING
		}
		server.SetServingStatus("", status)
		select {
		case <-ctx.Done():
			server.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
			return
		case <-ticker.C:
		}
	}
}

func Migrate(ctx context.Context, cfg config.Config) error {
	client := meili.New(cfg.Meilisearch)
	if err := waitForMeilisearch(ctx, client, cfg.Meilisearch.TaskTimeout); err != nil {
		return err
	}
	return client.Migrate(ctx, cfg.Enrichment.SynonymsFile)
}

func waitForMeilisearch(ctx context.Context, client *meili.Client, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := client.Health(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for meilisearch: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
