package health

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/onix-fun/search/service/internal/application"
)

type Probe struct {
	backend application.SearchBackend
	db      *pgxpool.Pool
	api     bool
	indexer bool
}

func NewProbe(searchBackend application.SearchBackend, db *pgxpool.Pool, api, indexer bool) *Probe {
	return &Probe{backend: searchBackend, db: db, api: api, indexer: indexer}
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
		if err := p.db.Ping(ctx); err != nil {
			return fmt.Errorf("postgres: %w", err)
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

func Mount(mux *http.ServeMux, logger *slog.Logger, probe *Probe) {
	mux.Handle("/livez", Handler(logger, probe))
	mux.Handle("/readyz", Handler(logger, probe))
	mux.HandleFunc("/metrics", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = writer.Write([]byte("# HELP search_service_up Whether the search service is running.\n# TYPE search_service_up gauge\nsearch_service_up 1\n"))
	})
}
