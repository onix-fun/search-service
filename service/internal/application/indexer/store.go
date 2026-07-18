package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/onix-fun/search/service/internal/domain"
)

type IngestStatus string

const (
	IngestAccepted  IngestStatus = "accepted"
	IngestDuplicate IngestStatus = "duplicate"
	IngestRejected  IngestStatus = "rejected"
)

type IngestResult struct {
	EventID string
	Status  IngestStatus
	Message string
}

type Task struct {
	ID       string
	Event    domain.IndexEvent
	Attempts int64
	Entity   string
}

type EmbeddingTask struct {
	ID, Collection, IndexName, DocumentID, ContentHash, ModelVersion, Embedder, Text string
	Revision                                                                         int64
	Attempts                                                                         int64
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Ingest(ctx context.Context, sourceService string, event domain.IndexEvent) (IngestResult, error) {
	event.SourceService = sourceService
	if err := event.Validate(); err != nil {
		if dlqErr := s.DeadLetter(ctx, event, 0, err.Error()); dlqErr != nil {
			return IngestResult{EventID: event.EventID, Status: IngestRejected, Message: err.Error()}, dlqErr
		}
		return IngestResult{EventID: event.EventID, Status: IngestRejected, Message: err.Error()}, nil
	}
	payload, err := event.CanonicalPayload()
	if err != nil {
		return IngestResult{}, err
	}
	digest, err := event.Digest()
	if err != nil {
		return IngestResult{}, err
	}
	occurredAt, _ := time.Parse(time.RFC3339, event.OccurredAt)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IngestResult{}, err
	}
	defer tx.Rollback(ctx)

	inboxID := uuid.Must(uuid.NewV7())
	tag, err := tx.Exec(ctx, `
		INSERT INTO inbox_events (id, source_service, event_id, operation, collection, document_id, revision, payload_json, payload_digest, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10)
		ON CONFLICT (source_service, event_id) DO NOTHING
	`, inboxID, event.SourceService, event.EventID, string(event.Operation), event.Collection, event.DocumentID, event.Revision, payload, digest, nullableTime(occurredAt))
	if err != nil {
		return IngestResult{}, err
	}
	if tag.RowsAffected() == 0 {
		return IngestResult{EventID: event.EventID, Status: IngestDuplicate}, tx.Commit(ctx)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO indexing_tasks (id, inbox_event_id, collection, document_id, operation, revision)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, uuid.Must(uuid.NewV7()), inboxID, event.Collection, event.DocumentID, string(event.Operation), event.Revision)
	if err != nil {
		return IngestResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return IngestResult{}, err
	}
	return IngestResult{EventID: event.EventID, Status: IngestAccepted}, nil
}

func (s *Store) LeaseTasks(ctx context.Context, limit int, leaseDuration time.Duration) ([]Task, error) {
	rows, err := s.pool.Query(ctx, `
		WITH picked AS (
			SELECT id
			FROM indexing_tasks
			WHERE (status IN ('pending', 'retry') AND next_attempt_at <= NOW())
			   OR (status IN ('leased', 'transforming', 'submitted_to_meili') AND leased_until <= NOW())
			ORDER BY created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE indexing_tasks t
		SET status = 'leased',
		    leased_until = NOW() + ($2 * INTERVAL '1 second'),
		    updated_at = NOW()
		FROM picked
		WHERE t.id = picked.id
		RETURNING t.id, t.attempts, (
			SELECT payload_json::text FROM inbox_events e WHERE e.id = t.inbox_event_id
		)
	`, limit, int64(leaseDuration.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		var task Task
		var payload string
		if err := rows.Scan(&task.ID, &task.Attempts, &payload); err != nil {
			return nil, err
		}
		event, err := domain.ParseEvent(payload)
		if err != nil {
			return nil, err
		}
		task.Event = event
		task.Entity = event.Collection
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (s *Store) MarkStage(ctx context.Context, id, status string) error {
	_, err := s.pool.Exec(ctx, `UPDATE indexing_tasks SET status=$2, updated_at=NOW() WHERE id=$1`, id, status)
	return err
}

func (s *Store) MarkIndexed(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE indexing_tasks
		SET status='indexed', leased_until=NULL, last_error=NULL, updated_at=NOW()
		WHERE id=$1
	`, id)
	return err
}

func (s *Store) EmbeddingCache(ctx context.Context, contentHash, modelVersion string) ([]float32, bool, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT vector_json FROM embedding_cache WHERE content_hash=$1 AND model_version=$2`, contentHash, modelVersion).Scan(&raw)
	if err == pgx.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var vector []float32
	if err := json.Unmarshal(raw, &vector); err != nil {
		return nil, false, err
	}
	return vector, true, nil
}

func (s *Store) SaveEmbeddingCache(ctx context.Context, contentHash, modelVersion string, vector []float32) error {
	raw, err := json.Marshal(vector)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO embedding_cache(content_hash,model_version,dimensions,vector_json) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(content_hash,model_version) DO NOTHING`, contentHash, modelVersion, len(vector), raw)
	return err
}

func (s *Store) QueueEmbedding(ctx context.Context, collection, indexName, documentID string, revision int64, contentHash, modelVersion, embedder, text string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO embedding_tasks(id,collection,index_name,document_id,revision,content_hash,model_version,embedder,semantic_text) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(collection,document_id,revision,model_version) DO NOTHING`, uuid.Must(uuid.NewV7()), collection, indexName, documentID, revision, contentHash, modelVersion, embedder, text)
	return err
}

func (s *Store) LeaseEmbeddingTasks(ctx context.Context, limit int, leaseDuration time.Duration) ([]EmbeddingTask, error) {
	rows, err := s.pool.Query(ctx, `WITH picked AS (SELECT id FROM embedding_tasks WHERE (status IN ('pending','retry') AND next_attempt_at<=NOW()) OR (status='leased' AND leased_until<=NOW()) ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED) UPDATE embedding_tasks t SET status='leased',leased_until=NOW()+($2*INTERVAL '1 second'),updated_at=NOW() FROM picked WHERE t.id=picked.id RETURNING t.id,t.collection,t.index_name,t.document_id,t.revision,t.content_hash,t.model_version,t.embedder,t.semantic_text,t.attempts`, limit, int64(leaseDuration.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []EmbeddingTask
	for rows.Next() {
		var task EmbeddingTask
		if err := rows.Scan(&task.ID, &task.Collection, &task.IndexName, &task.DocumentID, &task.Revision, &task.ContentHash, &task.ModelVersion, &task.Embedder, &task.Text, &task.Attempts); err != nil {
			return nil, err
		}
		result = append(result, task)
	}
	return result, rows.Err()
}

func (s *Store) CompleteEmbeddingTask(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE embedding_tasks SET status='indexed',leased_until=NULL,last_error=NULL,updated_at=NOW() WHERE id=$1`, id)
	return err
}
func (s *Store) RetryEmbeddingTask(ctx context.Context, task EmbeddingTask, maxRetries int64, cause error) error {
	attempts := task.Attempts + 1
	state := "retry"
	next := time.Now().Add(backoff(attempts))
	if attempts >= maxRetries {
		state = "dead"
		next = time.Now()
	}
	_, err := s.pool.Exec(ctx, `UPDATE embedding_tasks SET status=$2,attempts=$3,next_attempt_at=$4,leased_until=NULL,last_error=$5,updated_at=NOW() WHERE id=$1`, task.ID, state, attempts, next, cause.Error())
	return err
}

func (s *Store) RetryOrDead(ctx context.Context, task Task, maxRetries int64, cause error) error {
	attempts := task.Attempts + 1
	status := "retry"
	nextAttempt := time.Now().Add(backoff(attempts))
	if attempts >= maxRetries {
		status = "dead"
		nextAttempt = time.Now()
		if err := s.DeadLetter(ctx, task.Event, attempts, cause.Error()); err != nil {
			return err
		}
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE indexing_tasks
		SET status=$2, attempts=$3, next_attempt_at=$4, leased_until=NULL, last_error=$5, updated_at=NOW()
		WHERE id=$1
	`, task.ID, status, attempts, nextAttempt, cause.Error())
	return err
}

func (s *Store) DeadLetter(ctx context.Context, event domain.IndexEvent, attempts int64, reason string) error {
	payload, _ := json.Marshal(event)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO dead_letters (id, source_service, event_id, collection, document_id, revision, payload_json, attempts, reason)
		VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, 0), $7::jsonb, $8, $9)
	`, uuid.Must(uuid.NewV7()), event.SourceService, event.EventID, event.Collection, event.DocumentID, event.Revision, string(payload), attempts, reason)
	return err
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func (s *Store) Close() {}

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

func IsNoRows(err error) bool { return err == pgx.ErrNoRows }

func backoff(attempts int64) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	seconds := int64(1) << min(attempts-1, 5)
	return time.Duration(seconds) * time.Second
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func unknownEntity(entity string) error {
	return fmt.Errorf("unknown entity %s", entity)
}
