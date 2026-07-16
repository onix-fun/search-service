package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/onix-fun/search-service/internal/backend/meili"
	"github.com/onix-fun/search-service/internal/broker/rabbitmq"
	"github.com/onix-fun/search-service/internal/config"
	"github.com/onix-fun/search-service/internal/enrichment"
	"github.com/onix-fun/search-service/internal/health"
	"github.com/onix-fun/search-service/internal/httpapi"
	"github.com/onix-fun/search-service/internal/indexer"
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
	if err := meiliClient.Migrate(ctx, cfg.Collections); err != nil {
		return fmt.Errorf("migrate meilisearch index: %w", err)
	}
	processor := enrichment.New(cfg.Enrichment.Transliteration, cfg.Enrichment.Morphology)

	var broker *rabbitmq.Broker
	if runIndexer {
		var err error
		broker, err = rabbitmq.New(cfg.RabbitMQ, cfg.Collections, logger)
		if err != nil {
			return fmt.Errorf("connect to rabbitmq: %w", err)
		}
		defer broker.Close()
	}

	probe := health.NewProbe(meiliClient, redisClient, broker, runAPI, runIndexer)

	mux := http.NewServeMux()
	health.Mount(mux, logger, probe)
	if runAPI {
		mux.Handle("/", httpapi.New(meiliClient, cfg))
	}
	httpServer := &http.Server{Addr: cfg.Service.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errs := make(chan error, 3)
	go func() {
		logger.Info("health server started", "addr", cfg.Service.HTTPAddr)
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- fmt.Errorf("serve health HTTP: %w", err)
		}
	}()

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
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("health server shutdown failed", "error", err)
	}
	return runErr
}

func Migrate(ctx context.Context, cfg config.Config) error {
	client := meili.New(cfg.Meilisearch)
	if err := waitForMeilisearch(ctx, client, cfg.Meilisearch.TaskTimeout); err != nil {
		return err
	}
	return client.Migrate(ctx, cfg.Collections)
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
