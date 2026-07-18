package application

import (
	"context"

	"github.com/onix-fun/search/service/internal/domain"
)

type SearchBackend interface {
	Health(ctx context.Context) error
	Upsert(ctx context.Context, collection string, docs []domain.Document) error
	Delete(ctx context.Context, collection string, ids []string) error
	Search(ctx context.Context, collection string, request domain.SearchRequest) (domain.SearchResult, error)
}

type EmbeddingProvider interface {
	Embed(context.Context, string) ([]float32, error)
}
