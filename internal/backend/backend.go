package backend

import (
	"context"

	"github.com/company/search-service/internal/model"
)

type SearchBackend interface {
	Health(ctx context.Context) error
	Upsert(ctx context.Context, docs []model.Document) error
	Delete(ctx context.Context, ids []string) error
	Search(ctx context.Context, variants []string, limit int, entityType string) ([]string, error)
}
