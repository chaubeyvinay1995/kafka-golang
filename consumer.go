package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	// Set up Kafka consumer configuration
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",         // broker address
		"group.id":          "console-consumer-21564", // consumer group
		"auto.offset.reset": "earliest",               // earliest offset
	}
	// create kafka consumer
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Println("Error While connecting to the consumer group.")
	}
	// subscribe to target topi
	err = consumer.SubscribeTopics([]string{"golang-topic"}, nil)
	if err != nil {
		fmt.Println("Error while connecting")
	}

	// Consume messages

	for {

		msg, err := consumer.ReadMessage(-1)

		if err == nil {

			fmt.Printf("Received message: %s\n", string(msg.Value))

		} else {

			fmt.Printf("Error while consuming message: %v (%v)\n", err, msg)

		}

	}

	// Close Kafka consumer

	consumer.Close()

}
