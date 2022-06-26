package kafka

import (
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
)

type Consumer struct {
	MsgChan chan *ckafka.Message
}

func NewKafkaConsumer(msgChan chan *ckafka.Message) *Consumer {
	return &Consumer{
		MsgChan: msgChan,
	}
}

func (k *Consumer) Consumer() {
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KafkaBootstrapServers"),
		"group.id":          os.Getenv("KafkaConsumerGroupId"),
	}
	c, err := ckafka.NewConsumer(configMap)
	if err != nil {
		log.Fatal("Error consuming Kafka messages:" + err.Error())
	}
	topics := []string{os.Getenv("KafkaReadTopic")}
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		return
	}
	fmt.Println("Kafka Consumer has been started")
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			k.MsgChan <- msg
		}
	}
}
