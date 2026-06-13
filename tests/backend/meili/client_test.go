package meili_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/company/search-service/internal/backend/meili"
	"github.com/company/search-service/internal/config"
	"github.com/company/search-service/internal/model"
)

func TestUpsertWaitsForSucceededTask(t *testing.T) {
	var taskReads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/indexes/global_search/documents":
			writer.WriteHeader(http.StatusAccepted)
			_, _ = writer.Write([]byte(`{"taskUid":7}`))
		case "/tasks/7":
			status := "enqueued"
			if taskReads.Add(1) > 1 {
				status = "succeeded"
			}
			_ = json.NewEncoder(writer).Encode(map[string]string{"status": status})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	if err := client.Upsert(context.Background(), []model.Document{{ID: "uuid", UUID: "uuid"}}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if taskReads.Load() < 2 {
		t.Fatalf("task reads = %d, want at least 2", taskReads.Load())
	}
}

func TestSearchMergesVariantsWithoutDuplicates(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/multi-search" {
			http.NotFound(writer, request)
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"results": []any{
			map[string]any{"hits": []any{map[string]string{"uuid": "one"}, map[string]string{"uuid": "two"}}},
			map[string]any{"hits": []any{map[string]string{"uuid": "two"}, map[string]string{"uuid": "three"}}},
		}})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	uuids, err := client.Search(context.Background(), []string{"ivan", "иван"}, 3, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := len(uuids); got != 3 || uuids[0] != "one" || uuids[2] != "three" {
		t.Fatalf("Search() = %v", uuids)
	}
}

func TestSearchWithEntityFilter(t *testing.T) {
	var receivedFilter string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/multi-search" {
			http.NotFound(writer, request)
			return
		}
		var body struct {
			Queries []map[string]any `json:"queries"`
		}
		json.NewDecoder(request.Body).Decode(&body)
		if len(body.Queries) > 0 {
			if filter, ok := body.Queries[0]["filter"]; ok {
				receivedFilter = filter.(string)
			}
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"results": []any{
			map[string]any{"hits": []any{map[string]string{"uuid": "filtered"}}},
		}})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	_, err := client.Search(context.Background(), []string{"test"}, 10, "users")
	if err != nil {
		t.Fatal(err)
	}
	if receivedFilter != "entity_type = users" {
		t.Fatalf("filter = %q, want \"entity_type = users\"", receivedFilter)
	}
}

func newTestClient(host string) *meili.Client {
	return meili.New(config.MeilisearchConfig{
		Host: host, Index: "global_search", TaskPollInterval: time.Millisecond, TaskTimeout: time.Second,
	})
}
