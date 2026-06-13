package health

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/company/search-service/internal/backend"
	"github.com/company/search-service/internal/broker/rabbitmq"
)

type Probe struct {
	backend  backend.SearchBackend
	redis    *redis.Client
	rabbitmq *rabbitmq.Broker
	api      bool
	indexer  bool
}

func NewProbe(searchBackend backend.SearchBackend, redisClient *redis.Client, rmq *rabbitmq.Broker, api, indexer bool) *Probe {
	return &Probe{backend: searchBackend, redis: redisClient, rabbitmq: rmq, api: api, indexer: indexer}
}

func (p *Probe) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if p.api || p.indexer {
		if err := p.backend.Health(ctx); err != nil {
			return fmt.Errorf("meilisearch: %w", err)
		}
	}
	if p.indexer {
		if err := p.redis.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		if p.rabbitmq != nil {
			if err := p.rabbitmq.Health(ctx); err != nil {
				return fmt.Errorf("rabbitmq: %w", err)
			}
		}
	}
	return nil
}

func Handler(logger *slog.Logger, probe *Probe) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		if err := probe.Ready(request.Context()); err != nil {
			logger.Warn("readiness check failed", "error", err)
			http.Error(writer, "not ready", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	return mux
}
