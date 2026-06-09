package pubsub

import (
	"bytes"
	"encoding/gob"
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

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error {
	amqpChan, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return fmt.Errorf("could not subscribe to %s: %v", queueName, err)
	}

	err = amqpChan.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("could not set qos: %v", err)
	}

	deliveries, err := amqpChan.Consume(
		queue.Name,
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
			target, err := unmarshaller(delivery.Body)
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

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		simpleQueueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			err := json.Unmarshal(data, &target)
			return target, err
		},
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		simpleQueueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			buffer := bytes.NewBuffer(data)
			decoder := gob.NewDecoder(buffer)
			err := decoder.Decode(&target)
			return target, err
		},
	)
}
