package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {

	amqpChan, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not subscribe to %s: %v", queueName, err)
	}

	deliveries, err := amqpChan.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("could not consume: %v", err)
	}

	go func() {
		for delivery := range deliveries {
			var target T
			err := json.Unmarshal(delivery.Body, &target)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}

			switch handler(target) {
			case Ack:
				delivery.Ack(false)
				fmt.Println("Ack")
			case NackRequeue:
				delivery.Nack(false, true)
				fmt.Println("Nack Requeue")
			case NackDiscard:
				delivery.Nack(false, false)
				fmt.Println("Nack Discard")
			}
		}
	}()

	return nil
}
