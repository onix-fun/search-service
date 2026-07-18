package indexer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/onix-fun/search/service/internal/domain"
)

type RevisionDecision int

const (
	RevisionNew RevisionDecision = iota
	RevisionStale
	RevisionDuplicate
	RevisionConflict
)

type AppliedState struct {
	Revision int64
	Digest   string
	Payload  string
}

type RevisionStore struct {
	pool       *pgxpool.Pool
	collection string
}

func NewRevisionStore(pool *pgxpool.Pool, collection string) *RevisionStore {
	return &RevisionStore{pool: pool, collection: collection}
}

func (s *RevisionStore) Check(ctx context.Context, event domain.IndexEvent) (RevisionDecision, error) {
	state, ok, err := s.Get(ctx, event.DocumentID)
	if err != nil || !ok {
		return RevisionNew, err
	}
	digest, err := event.Digest()
	if err != nil {
		return RevisionNew, err
	}
	switch {
	case state.Revision > event.Revision:
		return RevisionStale, nil
	case state.Revision < event.Revision:
		return RevisionNew, nil
	case state.Digest == digest:
		return RevisionDuplicate, nil
	default:
		return RevisionConflict, nil
	}
}

func (s *RevisionStore) Save(ctx context.Context, event domain.IndexEvent) (RevisionDecision, error) {
	payload, err := event.CanonicalPayload()
	if err != nil {
		return RevisionNew, err
	}
	digest, err := event.Digest()
	if err != nil {
		return RevisionNew, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return RevisionNew, err
	}
	defer tx.Rollback(ctx)

	var currentRevision int64
	var currentDigest string
	err = tx.QueryRow(ctx, `
		SELECT revision, payload_digest
		FROM applied_revisions
		WHERE collection=$1 AND document_id=$2
		FOR UPDATE
	`, event.Collection, event.DocumentID).Scan(&currentRevision, &currentDigest)
	if err != nil && err != pgx.ErrNoRows {
		return RevisionNew, fmt.Errorf("load applied revision: %w", err)
	}
	if err == nil {
		switch {
		case currentRevision > event.Revision:
			return RevisionStale, tx.Commit(ctx)
		case currentRevision == event.Revision && currentDigest == digest:
			return RevisionDuplicate, tx.Commit(ctx)
		case currentRevision == event.Revision:
			return RevisionConflict, tx.Commit(ctx)
		}
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO applied_revisions (collection, document_id, revision, payload_digest, payload_json, updated_at)
		VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
		ON CONFLICT (collection, document_id) DO UPDATE
		SET revision=EXCLUDED.revision,
		    payload_digest=EXCLUDED.payload_digest,
		    payload_json=EXCLUDED.payload_json,
		    updated_at=NOW()
	`, event.Collection, event.DocumentID, event.Revision, digest, payload)
	if err != nil {
		return RevisionNew, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RevisionNew, err
	}
	return RevisionNew, nil
}

func (s *RevisionStore) Get(ctx context.Context, documentID string) (AppliedState, bool, error) {
	var state AppliedState
	err := s.pool.QueryRow(ctx, `
		SELECT revision, payload_digest, payload_json::text
		FROM applied_revisions
		WHERE collection=$1 AND document_id=$2
	`, s.collection, documentID).Scan(&state.Revision, &state.Digest, &state.Payload)
	if err == pgx.ErrNoRows {
		return AppliedState{}, false, nil
	}
	if err != nil {
		return AppliedState{}, false, fmt.Errorf("get revision: %w", err)
	}
	return state, true, nil
}
