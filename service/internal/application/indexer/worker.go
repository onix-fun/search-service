package indexer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/onix-fun/search/service/internal/application"
	"github.com/onix-fun/search/service/internal/application/enrichment"
	"github.com/onix-fun/search/service/internal/domain"
	"github.com/onix-fun/search/service/internal/platform/config"
)

type Worker struct {
	logger     *slog.Logger
	Cfg        config.Config
	Backend    application.SearchBackend
	enricher   *enrichment.Processor
	stores     map[string]*RevisionStore
	lease      *Lease
	tasks      *Store
	leader     atomic.Bool
	embeddings application.EmbeddingProvider
}

func (w *Worker) SetEmbeddingProvider(provider application.EmbeddingProvider) {
	w.embeddings = provider
}

type QueuedMessage struct {
	TaskID   string
	Event    domain.IndexEvent
	Attempts int64
	Entity   string
}

func NewWorker(cfg config.Config, logger *slog.Logger, searchBackend application.SearchBackend, enricher *enrichment.Processor, tasks *Store, pool *pgxpool.Pool) *Worker {
	stores := make(map[string]*RevisionStore, len(cfg.Collections))
	for _, entity := range cfg.Collections {
		stores[entity.Name] = NewRevisionStore(pool, entity.Name)
	}
	return &Worker{
		Cfg:      cfg,
		logger:   logger,
		Backend:  searchBackend,
		enricher: enricher,
		tasks:    tasks,
		stores:   stores,
		lease:    NewLease(pool, cfg.Indexer.LeaseKey, cfg.Indexer.LeaseDuration),
	}
}

func (w *Worker) IsLeader() bool {
	return w.leader.Load()
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		token, err := NewLeaseToken()
		if err != nil {
			return fmt.Errorf("generate lease token: %w", err)
		}
		acquired, err := w.lease.Acquire(ctx, token)
		if err != nil {
			w.logger.Error("leader lease acquisition failed", "error", err)
			if !sleepContext(ctx, w.Cfg.Indexer.LeaseRenew) {
				return nil
			}
			continue
		}
		if !acquired {
			if !sleepContext(ctx, w.Cfg.Indexer.LeaseRenew) {
				return nil
			}
			continue
		}

		w.logger.Info("indexer became leader")
		w.leader.Store(true)
		activeCtx, cancel := context.WithCancel(ctx)
		renewDone := make(chan struct{})
		go w.renewLease(activeCtx, cancel, token, renewDone)
		err = w.consume(activeCtx)
		cancel()
		<-renewDone
		w.leader.Store(false)

		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if releaseErr := w.lease.Release(releaseCtx, token); releaseErr != nil {
			w.logger.Warn("leader lease release failed", "error", releaseErr)
		}
		releaseCancel()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Error("leader loop stopped", "error", err)
		}
		if !sleepContext(ctx, time.Second) {
			return nil
		}
	}
}

func (w *Worker) renewLease(ctx context.Context, cancel context.CancelFunc, token string, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(w.Cfg.Indexer.LeaseRenew)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := w.lease.Refresh(ctx, token)
			if err != nil || !ok {
				w.logger.Warn("leader lease lost", "error", err)
				cancel()
				return
			}
		}
	}
}

func (w *Worker) consume(ctx context.Context) error {
	ticker := time.NewTicker(w.Cfg.Indexer.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		tasks, err := w.tasks.LeaseTasks(ctx, w.Cfg.Indexer.QueueSize, w.Cfg.Indexer.LeaseDuration)
		if err != nil {
			return fmt.Errorf("lease indexing tasks: %w", err)
		}
		w.processEmbeddingTasks(ctx)
		if len(tasks) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
			continue
		}
		messages := make([]QueuedMessage, 0, len(tasks))
		for _, task := range tasks {
			messages = append(messages, QueuedMessage{
				TaskID:   task.ID,
				Event:    task.Event,
				Attempts: task.Attempts,
				Entity:   task.Entity,
			})
		}
		w.ProcessBatch(ctx, messages)
	}
}

func (w *Worker) ProcessBatch(ctx context.Context, messages []QueuedMessage) {
	var chunk []QueuedMessage
	seen := make(map[string]struct{})
	var operation domain.Operation
	var collection string
	flush := func() {
		if len(chunk) > 0 {
			w.applyChunk(ctx, chunk)
		}
		chunk = nil
		clear(seen)
		operation = ""
		collection = ""
	}

	for _, message := range messages {
		if _, exists := seen[message.Event.DocumentID]; exists || (operation != "" && operation != message.Event.Operation) || (collection != "" && collection != message.Event.Collection) {
			flush()
		}
		store := w.Store(message.Entity)
		if store == nil {
			w.deadLetterMessage(ctx, message, 0, "unknown entity "+message.Entity)
			continue
		}
		decision, err := store.Check(ctx, message.Event)
		if err != nil {
			w.retryOrDLQ(ctx, message, err)
			continue
		}
		switch decision {
		case RevisionStale, RevisionDuplicate:
			w.markIndexed(ctx, message)
			continue
		case RevisionConflict:
			w.deadLetterMessage(ctx, message, 0, "conflicting payload for an applied revision")
			continue
		}
		operation = message.Event.Operation
		collection = message.Event.Collection
		seen[message.Event.DocumentID] = struct{}{}
		chunk = append(chunk, message)
	}
	flush()
}

func (w *Worker) applyChunk(ctx context.Context, messages []QueuedMessage) {
	if len(messages) == 0 || ctx.Err() != nil {
		return
	}
	for _, message := range messages {
		if err := w.tasks.MarkStage(ctx, message.TaskID, "transforming"); err != nil {
			w.retryOrDLQ(ctx, message, err)
			return
		}
	}
	var err error
	switch messages[0].Event.Operation {
	case domain.OperationUpsert:
		docs := make([]domain.Document, 0, len(messages))
		cfg, ok := w.Cfg.Collection(messages[0].Event.Collection)
		if !ok {
			err = fmt.Errorf("unknown collection %s", messages[0].Event.Collection)
			break
		}
		for _, message := range messages {
			document := w.enricher.Enrich(message.Event)
			if w.embeddings != nil && cfg.Embedder != "" && len(cfg.Semantic) > 0 {
				text := semanticDocumentText(document, cfg)
				if text != "" {
					digest := sha256.Sum256([]byte(text))
					hash := hex.EncodeToString(digest[:])
					vector, cached, cacheErr := w.tasks.EmbeddingCache(ctx, hash, w.Cfg.Embedding.Model)
					if cacheErr != nil {
						w.logger.Warn("embedding cache read failed", "error", cacheErr)
					}
					if !cached {
						vector, cacheErr = w.embeddings.Embed(ctx, "passage: "+text)
						if cacheErr == nil {
							_ = w.tasks.SaveEmbeddingCache(ctx, hash, w.Cfg.Embedding.Model, vector)
						}
					}
					if cacheErr != nil {
						w.logger.Warn("semantic enrichment unavailable; indexing lexical document", "document_id", message.Event.DocumentID, "error", cacheErr)
						if queueErr := w.tasks.QueueEmbedding(ctx, message.Event.Collection, cfg.Index, message.Event.DocumentID, message.Event.Revision, hash, w.Cfg.Embedding.Model, cfg.Embedder, text); queueErr != nil {
							w.logger.Warn("queue embedding retry failed", "document_id", message.Event.DocumentID, "error", queueErr)
						}
					} else if len(vector) > 0 {
						document["_vectors"] = map[string]any{cfg.Embedder: vector}
					}
				}
			}
			docs = append(docs, document)
		}
		err = w.Backend.Upsert(ctx, cfg.Index, docs)
	case domain.OperationDelete:
		ids := make([]string, 0, len(messages))
		for _, message := range messages {
			ids = append(ids, message.Event.DocumentID)
		}
		cfg, ok := w.Cfg.Collection(messages[0].Event.Collection)
		if !ok {
			err = fmt.Errorf("unknown collection %s", messages[0].Event.Collection)
			break
		}
		err = w.Backend.Delete(ctx, cfg.Index, ids)
	}
	if err != nil {
		for _, message := range messages {
			w.retryOrDLQ(ctx, message, err)
		}
		return
	}
	for _, message := range messages {
		if err := w.tasks.MarkStage(ctx, message.TaskID, "submitted_to_meili"); err != nil {
			w.retryOrDLQ(ctx, message, err)
			return
		}
	}
	for _, message := range messages {
		if ctx.Err() != nil {
			return
		}
		store := w.Store(message.Entity)
		if store == nil {
			w.deadLetterMessage(ctx, message, 0, "unknown entity "+message.Entity)
			continue
		}
		decision, err := store.Save(ctx, message.Event)
		if err != nil {
			w.retryOrDLQ(ctx, message, err)
			continue
		}
		if decision == RevisionConflict {
			w.deadLetterMessage(ctx, message, 0, "conflicting payload for an applied revision")
			continue
		}
		if decision == RevisionStale {
			if err := w.reconcile(ctx, store, message.Event.DocumentID); err != nil {
				w.retryOrDLQ(ctx, message, err)
				continue
			}
		}
		w.markIndexed(ctx, message)
	}
}

func (w *Worker) processEmbeddingTasks(ctx context.Context) {
	if w.embeddings == nil {
		return
	}
	tasks, err := w.tasks.LeaseEmbeddingTasks(ctx, w.Cfg.Indexer.QueueSize, w.Cfg.Indexer.LeaseDuration)
	if err != nil {
		w.logger.Warn("lease embedding tasks failed", "error", err)
		return
	}
	for _, task := range tasks {
		store := w.Store(task.Collection)
		if store == nil {
			_ = w.tasks.RetryEmbeddingTask(ctx, task, w.Cfg.Indexer.MaxRetries, fmt.Errorf("unknown collection"))
			continue
		}
		state, ok, stateErr := store.Get(ctx, task.DocumentID)
		if stateErr != nil {
			_ = w.tasks.RetryEmbeddingTask(ctx, task, w.Cfg.Indexer.MaxRetries, stateErr)
			continue
		}
		if !ok || state.Revision != task.Revision {
			_ = w.tasks.CompleteEmbeddingTask(ctx, task.ID)
			continue
		}
		vector, cached, embedErr := w.tasks.EmbeddingCache(ctx, task.ContentHash, task.ModelVersion)
		if embedErr == nil && !cached {
			vector, embedErr = w.embeddings.Embed(ctx, "passage: "+task.Text)
			if embedErr == nil {
				embedErr = w.tasks.SaveEmbeddingCache(ctx, task.ContentHash, task.ModelVersion, vector)
			}
		}
		if embedErr == nil {
			embedErr = w.Backend.Upsert(ctx, task.IndexName, []domain.Document{{"id": task.DocumentID, "_revision": task.Revision, "_vectors": map[string]any{task.Embedder: vector}}})
		}
		if embedErr != nil {
			_ = w.tasks.RetryEmbeddingTask(ctx, task, w.Cfg.Indexer.MaxRetries, embedErr)
			continue
		}
		_ = w.tasks.CompleteEmbeddingTask(ctx, task.ID)
	}
}

func semanticDocumentText(document domain.Document, cfg config.CollectionConfig) string {
	parts := []string{}
	for _, field := range cfg.Semantic {
		value, ok := document[field].(string)
		if !ok || value == "" {
			continue
		}
		repeat := 1
		if weight := cfg.SemanticWeights[field]; weight >= .9 {
			repeat = 2
		}
		for i := 0; i < repeat; i++ {
			parts = append(parts, value)
		}
	}
	return strings.Join(parts, "\n")
}

func (w *Worker) reconcile(ctx context.Context, store *RevisionStore, uuid string) error {
	for attempt := 0; attempt < 3; attempt++ {
		before, ok, err := store.Get(ctx, uuid)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		event, err := domain.ParseEvent(before.Payload)
		if err != nil {
			return fmt.Errorf("decode applied state for reconciliation: %w", err)
		}
		switch event.Operation {
		case domain.OperationUpsert:
			cfg, ok := w.Cfg.Collection(event.Collection)
			if !ok {
				return fmt.Errorf("unknown collection %s", event.Collection)
			}
			err = w.Backend.Upsert(ctx, cfg.Index, []domain.Document{w.enricher.Enrich(event)})
		case domain.OperationDelete:
			cfg, ok := w.Cfg.Collection(event.Collection)
			if !ok {
				return fmt.Errorf("unknown collection %s", event.Collection)
			}
			err = w.Backend.Delete(ctx, cfg.Index, []string{event.DocumentID})
		}
		if err != nil {
			return fmt.Errorf("reconcile %s: %w", uuid, err)
		}
		after, ok, err := store.Get(ctx, uuid)
		if err != nil {
			return err
		}
		if ok && before.Revision == after.Revision && before.Digest == after.Digest {
			return nil
		}
	}
	return fmt.Errorf("reconcile %s: applied state kept changing", uuid)
}

func (w *Worker) retryOrDLQ(ctx context.Context, message QueuedMessage, cause error) {
	if ctx.Err() != nil {
		return
	}
	w.logger.Warn("index event will be retried", "event_id", message.Event.EventID, "entity", message.Entity, "attempts", message.Attempts+1, "error", cause)
	if err := w.tasks.RetryOrDead(ctx, Task{ID: message.TaskID, Event: message.Event, Attempts: message.Attempts, Entity: message.Entity}, w.Cfg.Indexer.MaxRetries, cause); err != nil {
		w.logger.Error("failed to update retry state", "event_id", message.Event.EventID, "error", err)
	}
}

func (w *Worker) deadLetterMessage(ctx context.Context, message QueuedMessage, attempts int64, reason string) {
	w.logger.Warn("event moved to DLQ", "event_id", message.Event.EventID, "entity", message.Entity, "attempts", attempts, "reason", reason)
	if err := w.tasks.RetryOrDead(ctx, Task{ID: message.TaskID, Event: message.Event, Attempts: w.Cfg.Indexer.MaxRetries - 1, Entity: message.Entity}, w.Cfg.Indexer.MaxRetries, errors.New(reason)); err != nil {
		w.logger.Error("failed to mark dead letter event", "event_id", message.Event.EventID, "error", err)
	}
}

func (w *Worker) markIndexed(ctx context.Context, message QueuedMessage) {
	if err := w.tasks.MarkIndexed(ctx, message.TaskID); err != nil {
		w.logger.Error("failed to mark index task done", "event_id", message.Event.EventID, "task_id", message.TaskID, "error", err)
	}
}

func (w *Worker) Store(entity string) *RevisionStore {
	return w.stores[entity]
}

func shardIndex(uuid string, count int) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(uuid))
	return int(hash.Sum32() % uint32(count))
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
