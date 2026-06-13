package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Operation string

const (
	OperationUpsert Operation = "upsert"
	OperationDelete Operation = "delete"
)

type IndexEvent struct {
	EventID     string            `json:"event_id"`
	Operation   Operation         `json:"operation"`
	EntityType  string            `json:"entity_type"`
	UUID        string            `json:"uuid"`
	Revision    int64             `json:"revision"`
	Source      string            `json:"source,omitempty"`
	Title       string            `json:"title,omitempty"`
	Description string            `json:"description,omitempty"`
	Text        string            `json:"text,omitempty"`
	Keywords    []string          `json:"keywords,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	UpdatedAt   string            `json:"updated_at,omitempty"`
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
	if strings.TrimSpace(e.EntityType) == "" {
		return errors.New("entity_type is required")
	}
	if _, err := uuid.Parse(e.UUID); err != nil {
		return errors.New("uuid must be a valid UUID")
	}
	if e.Revision <= 0 {
		return errors.New("revision must be greater than zero")
	}
	if e.UpdatedAt != "" {
		if _, err := time.Parse(time.RFC3339, e.UpdatedAt); err != nil {
			return errors.New("updated_at must use RFC3339")
		}
	}

	switch e.Operation {
	case OperationDelete:
		return nil
	case OperationUpsert:
		if strings.TrimSpace(e.Title) == "" &&
			strings.TrimSpace(e.Description) == "" &&
			strings.TrimSpace(e.Text) == "" &&
			len(e.Keywords) == 0 {
			return errors.New("at least one searchable field is required for upsert")
		}
		return nil
	default:
		return fmt.Errorf("unknown operation %q", e.Operation)
	}
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
