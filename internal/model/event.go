package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Operation string

const (
	OperationUpsert Operation = "upsert"
	OperationDelete Operation = "delete"
)

// IndexEvent is the stable RabbitMQ contract consumed by search-service v1.
type IndexEvent struct {
	EventID    string         `json:"event_id"`
	Operation  Operation      `json:"operation"`
	Collection string         `json:"collection"`
	DocumentID string         `json:"document_id"`
	Revision   int64          `json:"revision"`
	Document   map[string]any `json:"document,omitempty"`
	OccurredAt string         `json:"occurred_at,omitempty"`
}

func ParseEvent(payload string) (IndexEvent, error) {
	var event IndexEvent
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		return IndexEvent{}, fmt.Errorf("decode payload: %w", err)
	}
	if err := event.Validate(); err != nil {
		return IndexEvent{}, err
	}
	return event, nil
}

func (e IndexEvent) Validate() error {
	if strings.TrimSpace(e.EventID) == "" {
		return errors.New("event_id is required")
	}
	if strings.TrimSpace(e.Collection) == "" {
		return errors.New("collection is required")
	}
	if strings.TrimSpace(e.DocumentID) == "" {
		return errors.New("document_id is required")
	}
	if e.Revision <= 0 {
		return errors.New("revision must be greater than zero")
	}
	if e.OccurredAt != "" {
		if _, err := time.Parse(time.RFC3339, e.OccurredAt); err != nil {
			return errors.New("occurred_at must use RFC3339")
		}
	}
	switch e.Operation {
	case OperationDelete:
		return nil
	case OperationUpsert:
		if len(e.Document) == 0 {
			return errors.New("document is required for upsert")
		}
		return nil
	default:
		return fmt.Errorf("unknown operation %q", e.Operation)
	}
}

func (e IndexEvent) SearchDocument() Document {
	doc := Document{"id": e.DocumentID, "_revision": e.Revision}
	for key, value := range e.Document {
		if key != "id" && key != "_revision" {
			doc[key] = value
		}
	}
	return doc
}

func (e IndexEvent) CanonicalPayload() (string, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return "", fmt.Errorf("encode canonical payload: %w", err)
	}
	return string(data), nil
}

func (e IndexEvent) Digest() (string, error) {
	payload, err := e.CanonicalPayload()
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(hash[:]), nil
}
