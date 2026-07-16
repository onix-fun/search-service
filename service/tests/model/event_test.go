package model_test

import (
	"testing"

	"github.com/onix-fun/search-service/internal/model"
)

func TestParseEvent(t *testing.T) {
	event, err := model.ParseEvent(`{"event_id":"01HY","collection":"users","operation":"upsert","document_id":"opaque-1","revision":42,"document":{"name":"Ivan"}}`)
	if err != nil {
		t.Fatalf("ParseEvent() error = %v", err)
	}
	if event.Revision != 42 {
		t.Fatalf("Revision = %d, want 42", event.Revision)
	}
}

func TestValidateRequiresCollection(t *testing.T) {
	event := model.IndexEvent{EventID: "01HY", Operation: model.OperationUpsert, DocumentID: "id", Revision: 1, Document: map[string]any{"name": "Ivan"}}
	if err := event.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want collection validation error")
	}
}

func TestValidateDeleteRequiresRevision(t *testing.T) {
	event := model.IndexEvent{EventID: "01HY", Collection: "users", Operation: model.OperationDelete, DocumentID: "opaque"}
	if err := event.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want revision validation error")
	}
}

func TestDigestIsStableForEquivalentEvent(t *testing.T) {
	event := model.IndexEvent{EventID: "01HY", Collection: "users", Operation: model.OperationUpsert, DocumentID: "opaque", Revision: 1, Document: map[string]any{"name": "Ivan"}}
	first, err := event.Digest()
	if err != nil {
		t.Fatal(err)
	}
	second, err := event.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("Digest() changed: %q != %q", first, second)
	}
}
