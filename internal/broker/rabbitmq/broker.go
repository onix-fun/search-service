package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/company/search-service/internal/config"
	"github.com/company/search-service/internal/model"
)

type EntityDelivery struct {
	Delivery amqp.Delivery
	Entity   string
}

type Broker struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	cfg      config.RabbitMQConfig
	entities []config.EntityConfig
	logger   *slog.Logger
	mu       sync.Mutex
}

func New(cfg config.RabbitMQConfig, entities []config.EntityConfig, logger *slog.Logger) (*Broker, error) {
	broker := &Broker{cfg: cfg, entities: entities, logger: logger}
	if err := broker.connect(); err != nil {
		return nil, err
	}
	return broker, nil
}

func (b *Broker) connect() error {
	conn, err := amqp.Dial(b.cfg.URL)
	if err != nil {
		return fmt.Errorf("connect to rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("open rabbitmq channel: %w", err)
	}
	if err := ch.Qos(b.cfg.PrefetchCount, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("set prefetch: %w", err)
	}
	if err := b.declareTopology(ch); err != nil {
		ch.Close()
		conn.Close()
		return err
	}
	b.conn = conn
	b.ch = ch
	return nil
}

func (b *Broker) declareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(b.cfg.DLQExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare DLQ exchange: %w", err)
	}
	if _, err := ch.QueueDeclare(b.cfg.DLQQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare DLQ queue: %w", err)
	}
	if err := ch.QueueBind(b.cfg.DLQQueue, b.cfg.DLQQueue, b.cfg.DLQExchange, false, nil); err != nil {
		return fmt.Errorf("bind DLQ queue: %w", err)
	}

	for _, entity := range b.entities {
		if err := ch.ExchangeDeclare(entity.Exchange, "direct", true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare exchange %s: %w", entity.Exchange, err)
		}
		dlqArgs := amqp.Table{"x-dead-letter-exchange": b.cfg.DLQExchange}
		if _, err := ch.QueueDeclare(entity.Queue, true, false, false, false, dlqArgs); err != nil {
			return fmt.Errorf("declare queue %s: %w", entity.Queue, err)
		}
		if err := ch.QueueBind(entity.Queue, entity.Queue, entity.Exchange, false, nil); err != nil {
			return fmt.Errorf("bind queue %s: %w", entity.Queue, err)
		}
	}
	return nil
}

func (b *Broker) Publish(ctx context.Context, entity string, event model.IndexEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	ec, ok := b.entityConfig(entity)
	if !ok {
		return fmt.Errorf("unknown entity %s", entity)
	}
	return b.publish(ctx, ec.Exchange, ec.Queue, event, nil)
}

func (b *Broker) Republish(ctx context.Context, entity string, event model.IndexEvent, retries int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	ec, ok := b.entityConfig(entity)
	if !ok {
		return fmt.Errorf("unknown entity %s", entity)
	}
	headers := amqp.Table{"x-retry-count": retries}
	return b.publish(ctx, ec.Exchange, ec.Queue, event, headers)
}

func (b *Broker) DeadLetter(ctx context.Context, event model.IndexEvent, attempts int64, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	headers := amqp.Table{
		"x-retry-count":       attempts,
		"x-death-reason":      reason,
		"x-failed-at":         time.Now().UTC().Format(time.RFC3339Nano),
		"x-original-event-id": event.EventID,
		"x-entity":            event.EntityType,
	}
	return b.publish(ctx, b.cfg.DLQExchange, b.cfg.DLQQueue, event, headers)
}

func (b *Broker) publish(ctx context.Context, exchange, routingKey string, event model.IndexEvent, headers amqp.Table) error {
	payload, err := event.CanonicalPayload()
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	msg := amqp.Publishing{
		ContentType:  "application/json",
		Body:         []byte(payload),
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
	}
	if headers != nil {
		msg.Headers = headers
	}
	return b.ch.PublishWithContext(ctx, exchange, routingKey, false, false, msg)
}

func (b *Broker) Consume() (<-chan EntityDelivery, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	merged := make(chan EntityDelivery, b.cfg.PrefetchCount)
	var wg sync.WaitGroup

	for _, entity := range b.entities {
		deliveries, err := b.ch.Consume(entity.Queue, entity.Name, false, false, false, false, nil)
		if err != nil {
			return nil, fmt.Errorf("consume from %s: %w", entity.Queue, err)
		}
		wg.Add(1)
		go func(entityName string, ch <-chan amqp.Delivery) {
			defer wg.Done()
			for delivery := range ch {
				merged <- EntityDelivery{Delivery: delivery, Entity: entityName}
			}
		}(entity.Name, deliveries)
	}

	go func() {
		wg.Wait()
		close(merged)
	}()

	return merged, nil
}

func (b *Broker) Ack(tag uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ch.Ack(tag, false)
}

func (b *Broker) Nack(tag uint64, requeue bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ch.Nack(tag, false, requeue)
}

func (b *Broker) Health(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn == nil || b.conn.IsClosed() {
		return fmt.Errorf("rabbitmq connection is closed")
	}
	return nil
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ch != nil {
		b.ch.Close()
	}
	if b.conn != nil {
		b.conn.Close()
	}
}

func (b *Broker) entityConfig(name string) (config.EntityConfig, bool) {
	for _, e := range b.entities {
		if e.Name == name {
			return e, true
		}
	}
	return config.EntityConfig{}, false
}
