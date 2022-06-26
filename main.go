package main

import (
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	kafka2 "github.com/dev-full-cycle/simulator/application/kafka"
	"github.com/dev-full-cycle/simulator/infra/kafka"
	"github.com/joho/godotenv"
	"log"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

func main() {
	msgChan := make(chan *ckafka.Message)
	consumer := kafka.NewKafkaConsumer(msgChan)
	go consumer.Consumer()
	for msg := range msgChan {
		go kafka2.Produce(msg)
		fmt.Println(string(msg.Value))
	}
}
