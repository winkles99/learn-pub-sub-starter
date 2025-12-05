package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublishJSON marshals val to JSON and publishes it to the given exchange/routing key.
// It sets ContentType to application/json.
func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	b, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        b,
		},
	)
}

// SimpleQueueType controls whether a queue is durable or transient.
type SimpleQueueType int

const (
	// Durable queues survive broker restarts and are not auto-deleted.
	Durable SimpleQueueType = iota
	// Transient queues are non-durable, exclusive, and auto-deleted.
	Transient
)

// DeclareAndBind opens a channel on the provided connection, ensures the
// exchange exists, declares a queue (durable or transient) and binds it to
// the provided routing key. It returns the created channel and queue so
// callers can consume from or publish to it. The caller is responsible for
// closing the returned channel when done.
func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("open channel: %w", err)
	}

	// Declare exchange (idempotent)
	if err := ch.ExchangeDeclare(
		exchange,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	); err != nil {
		ch.Close()
		return nil, amqp.Queue{}, fmt.Errorf("declare exchange: %w", err)
	}

	// Configure queue flags based on requested type
	durable := false
	autoDelete := true
	exclusive := true
	if queueType == Durable {
		durable = true
		autoDelete = false
		exclusive = false
	}

	q, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		return nil, amqp.Queue{}, fmt.Errorf("declare queue: %w", err)
	}

	if err := ch.QueueBind(q.Name, key, exchange, false, nil); err != nil {
		ch.Close()
		return nil, amqp.Queue{}, fmt.Errorf("bind queue: %w", err)
	}

	return ch, q, nil
}
