package grpcserver_test

import (
	"context"
	"testing"

	searchv1 "github.com/company/search-service/api/search/v1"
	"github.com/company/search-service/internal/enrichment"
	"github.com/company/search-service/internal/grpcserver"
	"github.com/company/search-service/internal/model"
)

type fakeBackend struct {
	variants   []string
	limit      int
	entityType string
}

func (f *fakeBackend) Health(context.Context) error                   { return nil }
func (f *fakeBackend) Upsert(context.Context, []model.Document) error { return nil }
func (f *fakeBackend) Delete(context.Context, []string) error         { return nil }
func (f *fakeBackend) Search(_ context.Context, variants []string, limit int, entityType string) ([]string, error) {
	f.variants = variants
	f.limit = limit
	f.entityType = entityType
	return []string{"uuid"}, nil
}

func TestSearchLimitsAndEmptyQuery(t *testing.T) {
	backend := &fakeBackend{}
	server := grpcserver.New(backend, enrichment.New(true, true), 20, 100)

	response, err := server.Search(context.Background(), &searchv1.SearchRequest{Query: "  ", Limit: 1000})
	if err != nil || len(response.Uuids) != 0 {
		t.Fatalf("empty Search() = %#v, %v", response, err)
	}
	response, err = server.Search(context.Background(), &searchv1.SearchRequest{Query: "Ivan", EntityType: "users", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if backend.limit != 100 || len(response.Uuids) != 1 || len(backend.variants) < 2 {
		t.Fatalf("Search() limit = %d, variants = %v, response = %#v", backend.limit, backend.variants, response)
	}
	if backend.entityType != "users" {
		t.Fatalf("entityType = %q, want users", backend.entityType)
	}
}
