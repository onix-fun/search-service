package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/onix-fun/search-service/internal/app"
	"github.com/onix-fun/search-service/internal/config"
)

// @title Search Service API
// @version 1.0
// @description Microservice for high-performance full-text search and indexing with Meilisearch.
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @contact.email support@swagger.io

// @license.name MIT
// @license.url https://opensource.org/licenses/MIT

// @host localhost:8080
// @BasePath /

// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name Authorization

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger, os.Args[1:]); err != nil {
		logger.Error("command failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: search-service <serve|migrate-index> [flags]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch args[0] {
	case "serve":
		flags := flag.NewFlagSet("serve", flag.ContinueOnError)
		configPath := flags.String("config", "", "path to YAML config")
		roleValue := flags.String("role", string(app.RoleAll), "runtime role: api, indexer or all")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		cfg, err := config.Load(*configPath)
		if err != nil {
			return err
		}
		role, err := app.ParseRole(*roleValue)
		if err != nil {
			return err
		}
		return app.Run(ctx, cfg, role, logger)
	case "migrate-index":
		flags := flag.NewFlagSet("migrate-index", flag.ContinueOnError)
		configPath := flags.String("config", "", "path to YAML config")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		cfg, err := config.Load(*configPath)
		if err != nil {
			return err
		}
		if err := app.Migrate(ctx, cfg); err != nil {
			return err
		}
		logger.Info("index migration completed", "index", cfg.Meilisearch.Index)
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
