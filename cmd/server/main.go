package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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

	gamelogic.PrintServerHelp()

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

	// Wait for interrupt (Ctrl+C) or termination signal.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	// Infinite loop to handle server commands.
	for {
		// Get user input.
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		cmd := input[0]
		switch cmd {
		case "help":
			gamelogic.PrintServerHelp()
		case "pause":
			ps := routing.PlayingState{IsPaused: true}
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, ps); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to publish pause: %v\n", err)
			} else {
				fmt.Println("Published pause")
			}
		case "resume":
			ps := routing.PlayingState{IsPaused: false}
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, ps); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to publish resume: %v\n", err)
			} else {
				fmt.Println("Published resume")
			}
		case "quit":
			fmt.Println("Shutting down...")
			return
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}
}
