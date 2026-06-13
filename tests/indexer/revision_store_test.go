package indexer_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/company/search-service/internal/indexer"
	"github.com/company/search-service/internal/model"
)

func TestRevisionStoreLatestWinsAndDetectsConflict(t *testing.T) {
	server := miniredis.RunT(t)
	store := indexer.NewRevisionStore(redis.NewClient(&redis.Options{Addr: server.Addr()}), "revision:")
	ctx := context.Background()
	event := testEvent(2, model.OperationDelete)

	if decision, err := store.Save(ctx, event); err != nil || decision != indexer.RevisionNew {
		t.Fatalf("Save(new) = %v, %v", decision, err)
	}
	if decision, err := store.Check(ctx, testEvent(1, model.OperationUpsert)); err != nil || decision != indexer.RevisionStale {
		t.Fatalf("Check(stale) = %v, %v", decision, err)
	}
	if decision, err := store.Check(ctx, event); err != nil || decision != indexer.RevisionDuplicate {
		t.Fatalf("Check(duplicate) = %v, %v", decision, err)
	}
	conflict := event
	conflict.EventID = "different"
	if decision, err := store.Check(ctx, conflict); err != nil || decision != indexer.RevisionConflict {
		t.Fatalf("Check(conflict) = %v, %v", decision, err)
	}
}

func testEvent(revision int64, operation model.Operation) model.IndexEvent {
	event := model.IndexEvent{
		EventID: "01HY", EntityType: "users", Operation: operation,
		UUID: "9dd2e47e-7a2d-4b99-b7a1-ff0d94b7e301", Revision: revision,
	}
	if operation == model.OperationUpsert {
		event.Source = "users"
		event.Title = "Ivan"
	}
	return event
}
