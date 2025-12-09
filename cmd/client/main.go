package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	fmt.Println("Starting Peril client...")

	// Prompt the user for a username using the shared gamelogic helper
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Username input failed: %v", err)
	}
	log.Printf("Username set to: %s", username)

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	// Declare and bind a transient queue using the helper in internal/pubsub.
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	ch, q, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		conn.Close()
		log.Fatalf("Failed to declare and bind queue: %v", err)
	}
	defer func() {
		if err := ch.Close(); err != nil {
			log.Printf("Error closing channel: %v", err)
		}
	}()

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}

	done := make(chan struct{})
	go func() {
		for d := range msgs {
			var ps routing.PlayingState
			if err := json.Unmarshal(d.Body, &ps); err != nil {
				log.Printf("Failed to unmarshal PlayingState: %v", err)
				continue
			}
			log.Printf("Received PlayingState: IsPaused=%v", ps.IsPaused)
		}
		close(done)
	}()

	ps := routing.PlayingState{IsPaused: false}
	if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, ps); err != nil {
		log.Printf("Failed to publish PlayingState from client: %v", err)
	} else {
		log.Println("Client published PlayingState (IsPaused=false)")
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigs:
		fmt.Println("Signal received, shutting down...")
	case <-done:
		fmt.Println("Message channel closed, exiting")
	}

	_ = ch.Close()
	_ = conn.Close()
}
