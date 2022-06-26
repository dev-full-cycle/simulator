package kafka

import (
	"encoding/json"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dev-full-cycle/simulator/application/route"
	"github.com/dev-full-cycle/simulator/infra/kafka"
	"log"
	"os"
	"time"
)

func Produce(msg *ckafka.Message) {
	producer := kafka.NewKafkaProducer()
	r := route.NewRoute()
	json.Unmarshal(msg.Value, &r)
	r.LoadPositions()
	positions, err := r.ExportJsonPositions()
	if err != nil {
		log.Println(err.Error())
	}
	for _, p := range positions {
		kafka.Publish(p, os.Getenv("KafkaProduceTopic"), producer)
		time.Sleep(time.Millisecond * 500)
	}
}
