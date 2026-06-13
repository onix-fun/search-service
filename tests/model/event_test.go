package model_test

import (
	"testing"

	"github.com/company/search-service/internal/model"
)

const validUUID = "9dd2e47e-7a2d-4b99-b7a1-ff0d94b7e301"

func TestParseEvent(t *testing.T) {
	event, err := model.ParseEvent(`{"event_id":"01HY","entity_type":"users","operation":"upsert","uuid":"` + validUUID + `","revision":42,"source":"users","title":"Ivan"}`)
	if err != nil {
		t.Fatalf("ParseEvent() error = %v", err)
	}
	if event.Revision != 42 {
		t.Fatalf("Revision = %d, want 42", event.Revision)
	}
}

func TestValidateRequiresEntityType(t *testing.T) {
	event := model.IndexEvent{EventID: "01HY", Operation: model.OperationUpsert, UUID: validUUID, Revision: 1}
	if err := event.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want entity_type validation error")
	}
}

func TestValidateDeleteRequiresRevision(t *testing.T) {
	event := model.IndexEvent{EventID: "01HY", EntityType: "users", Operation: model.OperationDelete, UUID: validUUID}
	if err := event.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want revision validation error")
	}
}

func TestDigestIsStableForEquivalentEvent(t *testing.T) {
	event := model.IndexEvent{EventID: "01HY", EntityType: "users", Operation: model.OperationUpsert, UUID: validUUID, Revision: 1, Source: "users", Title: "Ivan"}
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
