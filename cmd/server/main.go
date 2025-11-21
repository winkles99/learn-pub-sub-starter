package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	connStr := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to RabbitMQ: %v\n", err)
		os.Exit(1)
	}

	// Ensure the connection is closed when the program exits.
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Error closing connection: %v\n", err)
		}
	}()

	fmt.Println("Successfully connected to RabbitMQ")

	// Create a channel for publishing/consuming
	ch, err := conn.Channel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open a channel: %v\n", err)
		// Close connection and exit
		if cerr := conn.Close(); cerr != nil {
			fmt.Fprintf(os.Stderr, "Error closing connection after channel failure: %v\n", cerr)
		}
		os.Exit(1)
	}

	// Ensure the channel is closed when the program exits.
	defer func() {
		if err := ch.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Error closing channel: %v\n", err)
		}
	}()

	// Publish a PlayingState message (IsPaused = true) to the exchange/key.
	ps := routing.PlayingState{IsPaused: true}
	if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, ps); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to publish PlayingState: %v\n", err)
	} else {
		fmt.Println("Published PlayingState (IsPaused=true)")
	}

	// Wait for interrupt (Ctrl+C) or termination signal.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	<-sigs
	fmt.Println("Signal received, shutting down...")

	// Close the connection explicitly on shutdown (deferred close will also run).
	if err := conn.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Error closing connection during shutdown: %v\n", err)
	}
}
