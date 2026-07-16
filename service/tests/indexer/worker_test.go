package indexer_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/onix-fun/search-service/internal/broker/rabbitmq"
	"github.com/onix-fun/search-service/internal/config"
	"github.com/onix-fun/search-service/internal/enrichment"
	"github.com/onix-fun/search-service/internal/indexer"
	"github.com/onix-fun/search-service/internal/model"
)

type workerBackend struct {
	upsertErr error
	upserts   [][]model.Document
}

func (b *workerBackend) Health(context.Context) error                   { return nil }
func (b *workerBackend) Delete(context.Context, string, []string) error { return nil }
func (b *workerBackend) Search(context.Context, string, model.SearchRequest) (model.SearchResult, error) {
	return model.SearchResult{}, nil
}
func (b *workerBackend) Migrate(context.Context, []config.CollectionConfig) error { return nil }
func (b *workerBackend) Upsert(_ context.Context, _ string, docs []model.Document) error {
	b.upserts = append(b.upserts, docs)
	return b.upsertErr
}

type mockBroker struct {
	mu          sync.Mutex
	acked       int
	republished int
	dlqEvents   []model.IndexEvent
	dlqReasons  []string
	dlqAttempts []int64
}

func (m *mockBroker) Consume() (<-chan rabbitmq.EntityDelivery, error) { return nil, nil }
func (m *mockBroker) Nack(uint64, bool) error                          { return nil }
func (m *mockBroker) Ack(uint64) error {
	m.mu.Lock()
	m.acked++
	m.mu.Unlock()
	return nil
}
func (m *mockBroker) Republish(_ context.Context, _ string, _ model.IndexEvent, _ int64) error {
	m.mu.Lock()
	m.republished++
	m.mu.Unlock()
	return nil
}
func (m *mockBroker) DeadLetter(_ context.Context, event model.IndexEvent, attempts int64, reason string) error {
	m.mu.Lock()
	m.dlqEvents = append(m.dlqEvents, event)
	m.dlqReasons = append(m.dlqReasons, reason)
	m.dlqAttempts = append(m.dlqAttempts, attempts)
	m.mu.Unlock()
	return nil
}

func TestWorkerAcknowledgesAppliedEvent(t *testing.T) {
	worker, broker, message := newPendingEvent(t, 5)
	backend := worker.Backend.(*workerBackend)

	worker.ProcessBatch(context.Background(), []indexer.QueuedMessage{message})

	if len(backend.upserts) != 1 {
		t.Fatalf("upserts = %d, want 1", len(backend.upserts))
	}
	if broker.acked != 1 {
		t.Fatalf("broker acked = %d, want 1", broker.acked)
	}
	state, ok, err := worker.Store("users").Get(context.Background(), message.Event.DocumentID)
	if err != nil || !ok || state.Revision != 1 {
		t.Fatalf("revision state = %#v, %v, %v", state, ok, err)
	}
}

func TestWorkerMovesEventToDLQAfterRetries(t *testing.T) {
	worker, broker, message := newPendingEvent(t, 2)
	worker.Backend.(*workerBackend).upsertErr = errors.New("backend unavailable")

	worker.ProcessBatch(context.Background(), []indexer.QueuedMessage{message})
	if broker.republished != 1 {
		t.Fatalf("republished after first attempt = %d, want 1", broker.republished)
	}

	message.Retries = 1
	broker.republished = 0
	worker.ProcessBatch(context.Background(), []indexer.QueuedMessage{message})

	if len(broker.dlqEvents) != 1 {
		t.Fatalf("DLQ events = %d, want 1", len(broker.dlqEvents))
	}
	if broker.dlqAttempts[0] != 2 {
		t.Fatalf("DLQ attempts = %d, want 2", broker.dlqAttempts[0])
	}
}

func TestUnknownEntityGoesToDLQ(t *testing.T) {
	worker, broker, message := newPendingEvent(t, 5)
	message.Entity = "nonexistent"

	worker.ProcessBatch(context.Background(), []indexer.QueuedMessage{message})

	if len(broker.dlqEvents) != 1 {
		t.Fatalf("DLQ events = %d, want 1", len(broker.dlqEvents))
	}
}

func newPendingEvent(t *testing.T, maxRetries int64) (*indexer.Worker, *mockBroker, indexer.QueuedMessage) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	cfg := config.Defaults()
	cfg.Redis.Addr = server.Addr()
	cfg.Indexer.MaxRetries = maxRetries
	cfg.Collections = []config.CollectionConfig{
		{Name: "users", Index: "users", Exchange: "users.ex", Queue: "users.q", RoutingKey: "users.q", RevisionPrefix: "rev:users:"},
	}

	backend := &workerBackend{}
	broker := &mockBroker{}
	worker := indexer.NewWorker(cfg, testLogger(), backend, enrichment.New(true, true), broker, client)

	event := testWorkerEvent(1, model.OperationUpsert)
	payload, err := event.CanonicalPayload()
	if err != nil {
		t.Fatal(err)
	}
	delivery := amqp.Delivery{DeliveryTag: 1, Body: []byte(payload)}
	return worker, broker, indexer.QueuedMessage{Delivery: delivery, Payload: payload, Event: event, Entity: "users"}
}

func testWorkerEvent(revision int64, operation model.Operation) model.IndexEvent {
	event := model.IndexEvent{
		EventID: "01HY", Collection: "users", Operation: operation,
		DocumentID: "user-1", Revision: revision,
	}
	if operation == model.OperationUpsert {
		event.Document = map[string]any{"name": "Ivan"}
	}
	return event
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
