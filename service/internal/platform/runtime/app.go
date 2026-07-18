package runtime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/onix-fun/search/service/internal/adapters/grpcapi"
	"github.com/onix-fun/search/service/internal/adapters/health"
	"github.com/onix-fun/search/service/internal/adapters/inference"
	"github.com/onix-fun/search/service/internal/adapters/meili"
	"github.com/onix-fun/search/service/internal/application/enrichment"
	"github.com/onix-fun/search/service/internal/application/indexer"
	searchpb "github.com/onix-fun/search/service/internal/gen/search"
	"github.com/onix-fun/search/service/internal/platform/config"
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

	meiliClient := meili.New(cfg.Meilisearch)
	embeddingProvider := inference.New(cfg.Embedding.Endpoint, cfg.Embedding.Model, cfg.Embedding.Timeout)
	if err := waitForMeilisearch(ctx, meiliClient, cfg.Meilisearch.TaskTimeout); err != nil {
		return fmt.Errorf("wait for meilisearch: %w", err)
	}
	if err := meiliClient.Migrate(ctx, cfg.Collections); err != nil {
		return fmt.Errorf("migrate meilisearch index: %w", err)
	}
	if err := meiliClient.RetireIndexes(ctx, cfg.Meilisearch.RetiredIndexes); err != nil {
		return fmt.Errorf("retire meilisearch indexes: %w", err)
	}
	processor := enrichment.New(cfg.Enrichment.Transliteration, cfg.Enrichment.Morphology)

	if cfg.Database.AutoMigrate {
		migrationURL, err := databaseMigrationURL(cfg.Database.URL)
		if err != nil {
			return fmt.Errorf("configure search database migrator: %w", err)
		}
		migrator, err := migrate.New(cfg.Database.MigrationPath, migrationURL)
		if err != nil {
			return fmt.Errorf("create search migrator: %w", err)
		}
		if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("migrate search database: %w", err)
		}
	}
	poolConfig, err := pgxpool.ParseConfig(cfg.Database.URL)
	if err != nil {
		return fmt.Errorf("parse search database config: %w", err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = "search,public"
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("connect search database: %w", err)
	}
	defer pool.Close()
	store := indexer.NewStore(pool)

	probe := health.NewProbe(meiliClient, pool, runAPI, runIndexer)

	mux := http.NewServeMux()
	health.Mount(mux, logger, probe)
	httpServer := &http.Server{Addr: cfg.Service.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	var grpcServer *grpc.Server
	errs := make(chan error, 3)
	go func() {
		logger.Info("health server started", "addr", cfg.Service.HTTPAddr)
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- fmt.Errorf("serve health HTTP: %w", err)
		}
	}()

	if runAPI {
		server, err := newGRPCServer(cfg)
		if err != nil {
			return err
		}
		grpcServer = server
		searchpb.RegisterSearchServiceServer(grpcServer, grpcapi.New(meiliClient, store, cfg, embeddingProvider))
		listener, err := net.Listen("tcp", cfg.Service.GRPCAddr)
		if err != nil {
			return fmt.Errorf("listen grpc: %w", err)
		}
		go func() {
			logger.Info("gRPC server started", "addr", cfg.Service.GRPCAddr, "tls", cfg.Service.GRPCTLS)
			if err := grpcServer.Serve(listener); err != nil {
				errs <- fmt.Errorf("serve grpc: %w", err)
			}
		}()
	}

	if runIndexer {
		worker := indexer.NewWorker(cfg, logger, meiliClient, processor, store, pool)
		worker.SetEmbeddingProvider(embeddingProvider)
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
	if grpcServer != nil {
		grpcServer.GracefulStop()
	}
	return runErr
}

// databaseMigrationURL keeps golang-migrate metadata in public. PostgreSQL's
// default search_path starts with "$user"; because the database user and the
// application schema are both named search, the effective schema changes after
// V1 creates it. Without an explicit path, the next restart creates a second
// schema_migrations table and attempts to apply V1 again.
func databaseMigrationURL(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse database URL: %w", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", fmt.Errorf("database URL must use postgres or postgresql scheme")
	}
	query := parsed.Query()
	query.Set("search_path", "public")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func newGRPCServer(cfg config.Config) (*grpc.Server, error) {
	if !cfg.Service.GRPCTLS {
		return grpc.NewServer(), nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.Service.GRPCCertFile, cfg.Service.GRPCKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load grpc cert: %w", err)
	}
	ca, err := os.ReadFile(cfg.Service.GRPCClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read grpc client ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid grpc client ca")
	}
	return grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}))), nil
}

func Migrate(ctx context.Context, cfg config.Config) error {
	client := meili.New(cfg.Meilisearch)
	if err := waitForMeilisearch(ctx, client, cfg.Meilisearch.TaskTimeout); err != nil {
		return err
	}
	if err := client.Migrate(ctx, cfg.Collections); err != nil {
		return err
	}
	return client.RetireIndexes(ctx, cfg.Meilisearch.RetiredIndexes)
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
