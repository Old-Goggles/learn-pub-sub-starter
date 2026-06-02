package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connectionString := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}

	defer conn.Close()

	gamelogic.PrintServerHelp()

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Unable to open channel %v", err)
	}

	fmt.Println("Peril server is connected and ready to rumble!")

	err = pubsub.SubscribeGob[routing.GameLog](
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.Durable,
		func(log routing.GameLog) pubsub.Acktype {
			defer gamelogic.PrintServerHelp()
			err := gamelogic.WriteLog(log)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		},
	)
	if err != nil {
		log.Fatalf("could not subscribe to game_logs: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				})
			fmt.Println("Peril Server is paused, take a break.")

		case "resume":
			pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				})
			fmt.Println("Peril server is connected and ready to rumble!")

		case "quit":
			fmt.Println("The battle is over... shutting down Peril server.")
			return

		default:
			fmt.Println("Command Not Found")
		}
	}
}
