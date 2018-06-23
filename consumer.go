package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	//max partitions for each single topic, we have 3
	//255 is defined for future scale
	maxPartitions = 255
)

type partitionHandler struct {
	c  chan *kafka.Message
	pn int
}

var handlers []*partitionHandler

func (p *partitionHandler) proc() {
	for {
		select {
		case m := <-p.c:
			fmt.Printf("handle msg from partition %d, actual partition:%d, key:%s, value:%s\n", p.pn,
				m.TopicPartition.Partition, string(m.Key), string(m.Value))
		}
	}
}

func (p *partitionHandler) sendMsg(m *kafka.Message) {
	p.c <- m
}

func runPartitionHandlers() {
	handlers = make([]*partitionHandler, maxPartitions)
	for i := 0; i < maxPartitions; i++ {
		handlers[i] = new(partitionHandler)
		handlers[i].c = make(chan *kafka.Message, 10000)
		handlers[i].pn = i
		go handlers[i].proc()
	}
}

func main() {
	runPartitionHandlers()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "myGroup_chat_x",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"chat_x"}, nil)
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			handlers[msg.TopicPartition.Partition].sendMsg(msg)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}
	c.Close()
}
