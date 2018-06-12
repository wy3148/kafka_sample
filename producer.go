package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spaolacci/murmur3"
	"hash"
	"strconv"
)

var hashHandle hash.Hash32

func init() {
	hashHandle = murmur3.New32WithSeed(90230)
}

//
//	num is the partition numbers for the topic
//
func getPartition(v string, num int) uint32 {

	if num == 1 {
		return 0
	}
	hashHandle.Write([]byte(v))
	defer hashHandle.Reset()
	return hashHandle.Sum32() % uint32(num)
}

func main() {
	// addr := "localhost:9092"
	broker := sarama.NewBroker("localhost:9092")
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_0_0_0
	broker.Open(cfg)

	topic := &sarama.CreateTopicsRequest{
		TopicDetails: make(map[string]*sarama.TopicDetail),
	}

	topic.TopicDetails["chat"] = &sarama.TopicDetail{
		NumPartitions:     3,
		ReplicationFactor: 1,
	}

	res, err := broker.CreateTopics(topic)

	if err != nil {
		panic(err)
	}

	for k, v := range res.TopicErrors {
		fmt.Println(k)
		if v.Err != sarama.ErrNoError && v.ErrMsg != nil {
			fmt.Println(*v.ErrMsg)
		}
	}

	client, err := sarama.NewClient([]string{"localhost:9092"}, cfg)

	if err != nil {
		panic(err)
	}

	var num int
	if p, err := client.Partitions("chat"); err == nil {
		num = len(p)
		for _, id := range p {
			fmt.Println(id)
		}
	}

	cfg.Producer.Return.Successes = true

	producer, _ := sarama.NewAsyncProducer([]string{"localhost:9092"}, cfg)
	for i := 1; i < 1000000; i++ {
		topic := "chat"
		part := int32(getPartition(strconv.Itoa(i), num))
		fmt.Println(part)

		s := "hello:" + strconv.Itoa(i)
		producer.Input() <- &sarama.ProducerMessage{
			Topic:     topic,
			Partition: part,
			Value:     sarama.StringEncoder([]byte(s)),
		}
		select {
		case <-producer.Successes():
		case err := <-producer.Errors():
			fmt.Println("failure:", err.Error())
		}
	}
}
