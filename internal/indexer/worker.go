package indexer

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/company/search-service/internal/backend"
	"github.com/company/search-service/internal/broker/rabbitmq"
	"github.com/company/search-service/internal/config"
	"github.com/company/search-service/internal/enrichment"
	"github.com/company/search-service/internal/model"
)

type Broker interface {
	Consume() (<-chan rabbitmq.EntityDelivery, error)
	Ack(tag uint64) error
	Nack(tag uint64, requeue bool) error
	DeadLetter(ctx context.Context, event model.IndexEvent, attempts int64, reason string) error
	Republish(ctx context.Context, entity string, event model.IndexEvent, retries int64) error
}

type Worker struct {
	logger   *slog.Logger
	Cfg      config.Config
	Backend  backend.SearchBackend
	enricher *enrichment.Processor
	stores   map[string]*RevisionStore
	lease    *Lease
	broker   Broker
	leader   atomic.Bool
}

type QueuedMessage struct {
	Delivery amqp.Delivery
	Payload  string
	Event    model.IndexEvent
	Retries  int64
	Entity   string
}

func NewWorker(cfg config.Config, logger *slog.Logger, searchBackend backend.SearchBackend, enricher *enrichment.Processor, broker Broker, redisClient *redis.Client) *Worker {
	stores := make(map[string]*RevisionStore, len(cfg.Entities))
	for _, entity := range cfg.Entities {
		stores[entity.Name] = NewRevisionStore(redisClient, entity.RevisionPrefix)
	}
	return &Worker{
		Cfg:      cfg,
		logger:   logger,
		Backend:  searchBackend,
		enricher: enricher,
		broker:   broker,
		stores:   stores,
		lease:    NewLease(redisClient, cfg.Redis.LeaseKey, cfg.Redis.LeaseDuration),
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
			if !sleepContext(ctx, w.Cfg.Redis.LeaseRenew) {
				return nil
			}
			continue
		}
		if !acquired {
			if !sleepContext(ctx, w.Cfg.Redis.LeaseRenew) {
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
	ticker := time.NewTicker(w.Cfg.Redis.LeaseRenew)
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
	deliveries, err := w.broker.Consume()
	if err != nil {
		return fmt.Errorf("start rabbitmq consumer: %w", err)
	}

	shards := make([]chan QueuedMessage, w.Cfg.Indexer.Shards)
	var workers sync.WaitGroup
	for index := range shards {
		shards[index] = make(chan QueuedMessage, w.Cfg.Indexer.QueueSize)
		workers.Add(1)
		go func(messages <-chan QueuedMessage) {
			defer workers.Done()
			w.runShard(ctx, messages)
		}(shards[index])
	}
	defer func() {
		for _, shard := range shards {
			close(shard)
		}
		workers.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ed, ok := <-deliveries:
			if !ok {
				return errors.New("rabbitmq delivery channel closed")
			}
			event, err := model.ParseEvent(string(ed.Delivery.Body))
			if err != nil {
				w.logger.Warn("invalid event payload, sending to DLQ", "error", err, "entity", ed.Entity)
				if dlqErr := w.deadLetterRaw(ctx, model.IndexEvent{EntityType: ed.Entity}, ed.Delivery, 0, err.Error()); dlqErr != nil {
					return dlqErr
				}
				continue
			}
			if event.EntityType == "" {
				event.EntityType = ed.Entity
			}

			var retries int64
			if val, ok := ed.Delivery.Headers["x-retry-count"]; ok {
				switch v := val.(type) {
				case int64:
					retries = v
				case int32:
					retries = int64(v)
				case float64:
					retries = int64(v)
				}
			}

			index := shardIndex(event.UUID, len(shards))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case shards[index] <- QueuedMessage{Delivery: ed.Delivery, Payload: string(ed.Delivery.Body), Event: event, Retries: retries, Entity: ed.Entity}:
			}
		}
	}
}

func (w *Worker) runShard(ctx context.Context, messages <-chan QueuedMessage) {
	ticker := time.NewTicker(w.Cfg.Indexer.FlushInterval)
	defer ticker.Stop()
	batch := make([]QueuedMessage, 0, w.Cfg.Indexer.QueueSize)
	flush := func() {
		if len(batch) == 0 || ctx.Err() != nil {
			batch = batch[:0]
			return
		}
		w.ProcessBatch(ctx, batch)
		batch = batch[:0]
	}
	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-messages:
			if !ok {
				flush()
				return
			}
			batch = append(batch, message)
		case <-ticker.C:
			flush()
		}
	}
}

func (w *Worker) ProcessBatch(ctx context.Context, messages []QueuedMessage) {
	var chunk []QueuedMessage
	seen := make(map[string]struct{})
	var operation model.Operation
	flush := func() {
		if len(chunk) > 0 {
			w.applyChunk(ctx, chunk)
		}
		chunk = nil
		clear(seen)
		operation = ""
	}

	for _, message := range messages {
		if _, exists := seen[message.Event.UUID]; exists || (operation != "" && operation != message.Event.Operation) {
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
			w.ackLogged(message.Delivery.DeliveryTag)
			continue
		case RevisionConflict:
			w.deadLetterMessage(ctx, message, 0, "conflicting payload for an applied revision")
			continue
		}
		operation = message.Event.Operation
		seen[message.Event.UUID] = struct{}{}
		chunk = append(chunk, message)
	}
	flush()
}

func (w *Worker) applyChunk(ctx context.Context, messages []QueuedMessage) {
	if len(messages) == 0 || ctx.Err() != nil {
		return
	}
	var err error
	switch messages[0].Event.Operation {
	case model.OperationUpsert:
		docs := make([]model.Document, 0, len(messages))
		for _, message := range messages {
			docs = append(docs, w.enricher.Enrich(message.Event))
		}
		err = w.Backend.Upsert(ctx, docs)
	case model.OperationDelete:
		ids := make([]string, 0, len(messages))
		for _, message := range messages {
			ids = append(ids, message.Event.UUID)
		}
		err = w.Backend.Delete(ctx, ids)
	}
	if err != nil {
		for _, message := range messages {
			w.retryOrDLQ(ctx, message, err)
		}
		return
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
			if err := w.reconcile(ctx, store, message.Event.UUID); err != nil {
				w.retryOrDLQ(ctx, message, err)
				continue
			}
		}
		w.ackLogged(message.Delivery.DeliveryTag)
	}
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
		event, err := model.ParseEvent(before.Payload)
		if err != nil {
			return fmt.Errorf("decode applied state for reconciliation: %w", err)
		}
		switch event.Operation {
		case model.OperationUpsert:
			err = w.Backend.Upsert(ctx, []model.Document{w.enricher.Enrich(event)})
		case model.OperationDelete:
			err = w.Backend.Delete(ctx, []string{event.UUID})
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
	attempts := message.Retries + 1
	if attempts >= w.Cfg.Indexer.MaxRetries {
		w.deadLetterMessage(ctx, message, attempts, cause.Error())
		return
	}
	w.logger.Warn("index event will be retried", "event_id", message.Event.EventID, "entity", message.Entity, "attempts", attempts, "error", cause)
	if err := w.broker.Ack(message.Delivery.DeliveryTag); err != nil {
		w.logger.Error("failed to ack for retry republish", "event_id", message.Event.EventID, "error", err)
		return
	}
	if err := w.broker.Republish(ctx, message.Entity, message.Event, attempts); err != nil {
		w.logger.Error("failed to republish event for retry", "event_id", message.Event.EventID, "entity", message.Entity, "error", err)
	}
}

func (w *Worker) deadLetterMessage(ctx context.Context, message QueuedMessage, attempts int64, reason string) {
	w.logger.Warn("event moved to DLQ", "event_id", message.Event.EventID, "entity", message.Entity, "attempts", attempts, "reason", reason)
	if err := w.broker.Ack(message.Delivery.DeliveryTag); err != nil {
		w.logger.Error("failed to ack DLQ event", "event_id", message.Event.EventID, "error", err)
	}
	if err := w.broker.DeadLetter(ctx, message.Event, attempts, reason); err != nil {
		w.logger.Error("failed to write DLQ event", "event_id", message.Event.EventID, "error", err)
	}
}

func (w *Worker) deadLetterRaw(ctx context.Context, event model.IndexEvent, delivery amqp.Delivery, attempts int64, reason string) error {
	if err := w.broker.DeadLetter(ctx, event, attempts, reason); err != nil {
		return fmt.Errorf("dead letter event: %w", err)
	}
	return w.broker.Ack(delivery.DeliveryTag)
}

func (w *Worker) ackLogged(tag uint64) {
	if err := w.broker.Ack(tag); err != nil {
		w.logger.Error("failed to acknowledge event", "delivery_tag", tag, "error", err)
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
