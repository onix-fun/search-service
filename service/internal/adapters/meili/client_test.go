package meili

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/onix-fun/search/service/internal/platform/config"
)

func TestMigrateOptsExistingDocumentsOutBeforeAddingUserProvidedEmbedder(t *testing.T) {
	t.Helper()
	var vectorUpdateSeen bool
	var settingsUpdateSeen bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/indexes/posts":
			_, _ = writer.Write([]byte(`{"uid":"posts"}`))
		case request.Method == http.MethodGet && request.URL.Path == "/indexes/posts/settings/embedders":
			_, _ = writer.Write([]byte(`{}`))
		case request.Method == http.MethodGet && request.URL.Path == "/indexes/posts/documents":
			_, _ = writer.Write([]byte(`{"results":[{"id":"post-1"}],"offset":0,"limit":1000,"total":1}`))
		case request.Method == http.MethodPut && request.URL.Path == "/indexes/posts/documents":
			var updates []map[string]any
			if err := json.NewDecoder(request.Body).Decode(&updates); err != nil {
				t.Fatalf("decode vector update: %v", err)
			}
			vectors := updates[0]["_vectors"].(map[string]any)
			if value, exists := vectors["e5"]; !exists || value != nil {
				t.Fatalf("expected explicit null vector, got %#v", vectors)
			}
			vectorUpdateSeen = true
			writer.WriteHeader(http.StatusAccepted)
			_, _ = writer.Write([]byte(`{"taskUid":1}`))
		case request.Method == http.MethodPatch && request.URL.Path == "/indexes/posts/settings":
			if !vectorUpdateSeen {
				t.Fatal("embedder settings were applied before existing documents opted out")
			}
			settingsUpdateSeen = true
			writer.WriteHeader(http.StatusAccepted)
			_, _ = writer.Write([]byte(`{"taskUid":2}`))
		case request.Method == http.MethodGet && request.URL.Path == "/tasks/1":
			_, _ = writer.Write([]byte(`{"uid":1,"status":"succeeded"}`))
		case request.Method == http.MethodGet && request.URL.Path == "/tasks/2":
			_, _ = writer.Write([]byte(`{"uid":2,"status":"succeeded"}`))
		default:
			t.Fatalf("unexpected request: %s %s", request.Method, request.URL.String())
		}
	}))
	defer server.Close()

	client := New(config.MeilisearchConfig{
		Host:             server.URL,
		TaskPollInterval: time.Millisecond,
		TaskTimeout:      time.Second,
	})
	err := client.Migrate(context.Background(), []config.CollectionConfig{{
		Name: "posts", Index: "posts", Searchable: []string{"content"}, Returnable: []string{"content"}, Embedder: "e5",
	}})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if !settingsUpdateSeen {
		t.Fatal("expected settings update")
	}
}

func TestMigrateWaitsForIndexCreationBeforeReadingSettings(t *testing.T) {
	var indexReady bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/indexes/posts":
			if !indexReady {
				writer.WriteHeader(http.StatusNotFound)
				_, _ = writer.Write([]byte(`{"message":"Index posts not found"}`))
				return
			}
			_, _ = writer.Write([]byte(`{"uid":"posts"}`))
		case request.Method == http.MethodPost && request.URL.Path == "/indexes":
			writer.WriteHeader(http.StatusAccepted)
			_, _ = writer.Write([]byte(`{"taskUid":0}`))
		case request.Method == http.MethodGet && request.URL.Path == "/tasks/0":
			indexReady = true
			_, _ = writer.Write([]byte(`{"uid":0,"status":"succeeded"}`))
		case request.Method == http.MethodGet && request.URL.Path == "/indexes/posts/settings/embedders":
			if !indexReady {
				t.Fatal("embedder settings were read before index creation completed")
			}
			_, _ = writer.Write([]byte(`{}`))
		case request.Method == http.MethodGet && request.URL.Path == "/indexes/posts/documents":
			_, _ = writer.Write([]byte(`{"results":[],"offset":0,"limit":1000,"total":0}`))
		case request.Method == http.MethodPatch && request.URL.Path == "/indexes/posts/settings":
			writer.WriteHeader(http.StatusAccepted)
			_, _ = writer.Write([]byte(`{"taskUid":1}`))
		case request.Method == http.MethodGet && request.URL.Path == "/tasks/1":
			_, _ = writer.Write([]byte(`{"uid":1,"status":"succeeded"}`))
		default:
			t.Fatalf("unexpected request: %s %s", request.Method, request.URL.String())
		}
	}))
	defer server.Close()

	client := New(config.MeilisearchConfig{Host: server.URL, TaskPollInterval: time.Millisecond, TaskTimeout: time.Second})
	if err := client.Migrate(context.Background(), []config.CollectionConfig{{Name: "posts", Index: "posts", Embedder: "e5"}}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

func TestMigratePreservesVectorsWhenEmbedderAlreadyExists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/indexes/posts":
			_, _ = writer.Write([]byte(`{"uid":"posts"}`))
		case request.Method == http.MethodGet && request.URL.Path == "/indexes/posts/settings/embedders":
			_, _ = writer.Write([]byte(`{"e5":{"source":"userProvided","dimensions":384}}`))
		case request.Method == http.MethodPatch && request.URL.Path == "/indexes/posts/settings":
			writer.WriteHeader(http.StatusAccepted)
			_, _ = writer.Write([]byte(`{"taskUid":1}`))
		case request.Method == http.MethodGet && request.URL.Path == "/tasks/1":
			_, _ = writer.Write([]byte(`{"uid":1,"status":"succeeded"}`))
		case request.Method == http.MethodPut && request.URL.Path == "/indexes/posts/documents":
			t.Fatal("existing vectors must not be reset on restart")
		default:
			t.Fatalf("unexpected request: %s %s", request.Method, request.URL.String())
		}
	}))
	defer server.Close()

	client := New(config.MeilisearchConfig{Host: server.URL, TaskPollInterval: time.Millisecond, TaskTimeout: time.Second})
	if err := client.Migrate(context.Background(), []config.CollectionConfig{{Name: "posts", Index: "posts", Embedder: "e5"}}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

func TestRetireIndexesIgnoresIndexesThatAreAlreadyAbsent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Path == "/indexes/comments" {
			writer.WriteHeader(http.StatusNotFound)
			_, _ = writer.Write([]byte(`{"message":"Index comments not found"}`))
			return
		}
		t.Fatalf("unexpected request: %s %s", request.Method, request.URL.String())
	}))
	defer server.Close()

	client := New(config.MeilisearchConfig{Host: server.URL, TaskPollInterval: time.Millisecond, TaskTimeout: time.Second})
	if err := client.RetireIndexes(context.Background(), []string{"comments"}); err != nil {
		t.Fatalf("retire absent index: %v", err)
	}
}
