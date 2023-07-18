package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("Running the producer")
	config := &kafka.ConfigMap{"bootstrap.servers": "localhost:9092"}
	producer, err := kafka.NewProducer(config)
	topic := "golang-topic"
	if err != nil {
		fmt.Println("Error while connecting to kafka.")
	}
	// now write message to kafka topic
	for i := 0; i < 20; i++ {
		value := fmt.Sprintf("message-%d", i)
		err := producer.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(value)},
			nil)
		if err != nil {
			fmt.Println("Error while sending message")
		} else {
			fmt.Printf("Produced message %d: %s\n\n", i, value)
		}
	}
	producer.Flush(15 * 100)
	producer.Close()
}
