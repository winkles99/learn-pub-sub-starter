package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SimpleQueueType controls whether a queue is durable or transient.
type SimpleQueueType int

const (
	// Durable queues survive broker restarts and are not auto-deleted.
	Durable SimpleQueueType = iota
	// Transient queues are non-durable, exclusive, and auto-deleted.
	Transient
)

// AckType represents how to acknowledge a message.
type AckType int

const (
	// Ack acknowledges the message successfully.
	Ack AckType = iota
	// NackRequeue rejects the message and requeues it.
	NackRequeue
	// NackDiscard rejects the message without requeuing.
	NackDiscard
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

// PublishGob encodes val to gob and publishes it to the given exchange/routing key.
// It sets ContentType to application/gob.
func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(val); err != nil {
		return fmt.Errorf("gob encode: %w", err)
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        buf.Bytes(),
		},
	)
}

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
	return DeclareAndBindWithExchangeType(conn, exchange, "direct", queueName, key, queueType)
}

// DeclareAndBindWithExchangeType is like DeclareAndBind but allows specifying the exchange type.
func DeclareAndBindWithExchangeType(
	conn *amqp.Connection,
	exchange,
	exchangeType,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to open channel: %w", err)
	}

	// Declare exchange (idempotent)
	if err := ch.ExchangeDeclare(
		exchange,
		exchangeType,
		true,
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

	// Configure dead letter exchange
	args := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}

	q, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,
		args,
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

// SubscribeJSON declares a queue, binds it to an exchange, and continuously consumes
// JSON messages from the queue. For each message, it unmarshals the JSON body into
// a value of type T and calls the handler function with that value.
// The function blocks indefinitely, listening for messages until the connection is closed.
func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	unmarshaller := func(data []byte) (T, error) {
		var val T
		err := json.Unmarshal(data, &val)
		return val, err
	}
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaller)
}

// SubscribeJSONWithExchangeType allows specifying exchange type (e.g., topic).
func SubscribeJSONWithExchangeType[T any](
	conn *amqp.Connection,
	exchange,
	exchangeType,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	unmarshaller := func(data []byte) (T, error) {
		var val T
		err := json.Unmarshal(data, &val)
		return val, err
	}
	return subscribeWithExchangeType(conn, exchange, exchangeType, queueName, key, queueType, handler, unmarshaller)
}

// SubscribeGob declares a queue, binds it to an exchange, and continuously consumes
// gob-encoded messages from the queue. For each message, it decodes the gob body into
// a value of type T and calls the handler function with that value.
// The function blocks indefinitely, listening for messages until the connection is closed.
func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	unmarshaller := func(data []byte) (T, error) {
		var val T
		buf := bytes.NewReader(data)
		dec := gob.NewDecoder(buf)
		err := dec.Decode(&val)
		return val, err
	}
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaller)
}

// SubscribeGobWithExchangeType allows specifying exchange type (e.g., topic).
func SubscribeGobWithExchangeType[T any](
	conn *amqp.Connection,
	exchange,
	exchangeType,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	unmarshaller := func(data []byte) (T, error) {
		var val T
		buf := bytes.NewReader(data)
		dec := gob.NewDecoder(buf)
		err := dec.Decode(&val)
		return val, err
	}
	return subscribeWithExchangeType(conn, exchange, exchangeType, queueName, key, queueType, handler, unmarshaller)
}

// subscribe is a generic helper function that handles the common subscription logic.
func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}
	return subscribeWithChannel(ch, q, handler, unmarshaller)
}

// subscribeWithExchangeType is a generic helper function that handles subscription with custom exchange type.
func subscribeWithExchangeType[T any](
	conn *amqp.Connection,
	exchange,
	exchangeType,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, q, err := DeclareAndBindWithExchangeType(conn, exchange, exchangeType, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}
	return subscribeWithChannel(ch, q, handler, unmarshaller)
}

// subscribeWithChannel handles message consumption and acknowledgment.
func subscribeWithChannel[T any](
	ch *amqp.Channel,
	q amqp.Queue,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume messages: %w", err)
	}

	go func() {
		defer ch.Close()
		for d := range msgs {
			val, err := unmarshaller(d.Body)
			if err != nil {
				fmt.Printf("Error unmarshaling message: %v\n", err)
				d.Ack(false)
				continue
			}
			ackType := handler(val)
			switch ackType {
			case Ack:
				fmt.Println("Acknowledging message")
				d.Ack(false)
			case NackRequeue:
				fmt.Println("Negative acknowledging message (requeue)")
				d.Nack(false, true)
			case NackDiscard:
				fmt.Println("Negative acknowledging message (discard)")
				d.Nack(false, false)
			}
		}
	}()

	return nil
}
