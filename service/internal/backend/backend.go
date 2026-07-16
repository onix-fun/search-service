package backend

import (
	"context"

	"github.com/onix-fun/search-service/internal/config"
	"github.com/onix-fun/search-service/internal/model"
)

type SearchBackend interface {
	Health(ctx context.Context) error
	Upsert(ctx context.Context, collection string, docs []model.Document) error
	Delete(ctx context.Context, collection string, ids []string) error
	Search(ctx context.Context, collection string, request model.SearchRequest) (model.SearchResult, error)
	Migrate(ctx context.Context, collections []config.CollectionConfig) error
}
