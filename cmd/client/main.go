package main

import (
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
	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		conn.Close()
		log.Fatalf("Failed to declare and bind queue: %v", err)
	}

	// Create a new game state for the player
	gameState := gamelogic.NewGameState(username)

	// Subscribe to pause messages using the new handler
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("pause.%s", username),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to pause messages: %v", err)
	}

	// Create a channel for publishing
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to create channel: %v", err)
	}
	defer ch.Close()

	// Subscribe to move messages from all other players via the topic exchange
	err = pubsub.SubscribeJSONWithExchangeType(
		conn,
		routing.ExchangePerilTopic,
		"topic",
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.Transient,
		handlerMove(ch, gameState),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to move messages: %v", err)
	}

	ps := routing.PlayingState{IsPaused: false}
	if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, ps); err != nil {
		log.Printf("Failed to publish PlayingState from client: %v", err)
	} else {
		log.Println("Client published PlayingState (IsPaused=false)")
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	go func() {
		for {
			select {
			case <-sigs:
				fmt.Println("Signal received, shutting down...")
				return
			default:
				words := gamelogic.GetInput()

				if len(words) == 0 {
					continue
				}

				switch words[0] {
				case "spawn":
					if len(words) < 3 {
						fmt.Println("spawn requires 2 arguments: location and unit type")
						fmt.Println("Example: spawn europe infantry")
						continue
					}
					err := gameState.CommandSpawn(words)
					if err != nil {
						log.Printf("Spawn failed: %v", err)
						continue
					}
					fmt.Println("Unit spawned successfully")
				case "move":
					if len(words) < 3 {
						fmt.Println("move requires 2 arguments: destination and unit ID")
						fmt.Println("Example: move europe 1")
						continue
					}
					mv, err := gameState.CommandMove(words)
					if err != nil {
						log.Printf("Move failed: %v", err)
						continue
					}
					// Publish the move to other players via topic exchange
					if err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, gameState.GetUsername()), mv); err != nil {
						log.Printf("Failed to publish move: %v", err)
						continue
					}
					log.Println("Move published successfully")
					fmt.Println("Unit moved successfully")
				case "status":
					gameState.CommandStatus()
				case "spam":
					fmt.Println("Spamming not allowed yet!")
				case "help":
					gamelogic.PrintClientHelp()
				case "quit":
					gamelogic.PrintQuit()
					return
				default:
					fmt.Printf("Unknown command: %s\n", words[0])
				}
			}
		}
	}()

	select {
	case <-sigs:
		fmt.Println("Signal received, shutting down...")
	}

	_ = ch.Close()
	_ = conn.Close()
}

// handlerPause returns a handler function that processes PlayingState messages.
// It logs the pause state received from the server.
func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

// handlerMove returns a handler that processes ArmyMove messages from other players.
// It updates local state and re-prompts the user.
func handlerMove(ch *amqp.Channel, gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(mv gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(mv)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			// Publish war recognition message
			war := gamelogic.RecognitionOfWar{
				Attacker: mv.Player,
				Defender: gs.Player,
			}
			routingKey := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.Player.Username)
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routingKey, war); err != nil {
				log.Printf("Failed to publish war recognition: %v", err)
			}
			return pubsub.NackRequeue
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}
