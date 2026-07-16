package indexer_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/onix-fun/search-service/internal/indexer"
	"github.com/onix-fun/search-service/internal/model"
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
		EventID: "01HY", Collection: "users", Operation: operation,
		DocumentID: "user-1", Revision: revision,
	}
	if operation == model.OperationUpsert {
		event.Document = map[string]any{"name": "Ivan"}
	}
	return event
}
