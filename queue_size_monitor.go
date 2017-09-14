package main

import (
	"fmt"
	"sync"
	"bytes"
	"encoding/binary"
	"log"
	"github.com/Shopify/sarama"
)

// ConsumerOffsetTopic : provides the topic name of the Offset Topic.
const ConsumerOffsetTopic = "__consumer_offsets"

// QueueSizeMonitor : Defines the type for Kafka Queue Size 
// Monitor implementation using Sarama.
type QueueSizeMonitor struct {
	Client                    sarama.Client
	wgConsumerMessages        sync.WaitGroup
	ConsumerOffsetChannel     chan *PartitionOffset
	BrokerOffsetChannel       chan *PartitionOffset
}

// NewQueueSizeMonitor : Returns a QueueSizeMonitor with an initialized client
// based on the comma-separated brokers (eg. "localhost:9092")
func NewQueueSizeMonitor(brokers []string) (*QueueSizeMonitor, error) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(brokers, config)
	qsm := &QueueSizeMonitor{}
	qsm.Client = client
	return qsm, err
}

// GetConsumerOffsets : Subcribes to Offset Topic and parses messages to 
// obtains Consumer Offsets.
func (qsm *QueueSizeMonitor) GetConsumerOffsets() {
	log.Println("Started getting consumer partition offsets...")
	
	partitions, err := qsm.Client.Partitions(ConsumerOffsetTopic)
	if err != nil {
		log.Fatalln("Error occured while getting client partitions.", err)
		return
	}

	consumer, err := sarama.NewConsumerFromClient(qsm.Client)
	if err != nil {
		log.Fatalln("Error occured while creating new client consumer.", err)
		return
	}

	partitionsConsumers := make([]sarama.PartitionConsumer, len(partitions))
	pOffsetChannel := make(chan *PartitionOffset)

	getConsumerMessages := func(consumer sarama.PartitionConsumer) {
		defer qsm.wgConsumerMessages.Done()
		for message := range consumer.Messages() {
			log.Println("Message received.", message)
			qsm.wgConsumerMessages.Add(1)
			go qsm.formatConsumerOffsetMessage(message)
		}
	}

	getConsumerErrors := func(consumer sarama.PartitionConsumer) {
		defer qsm.wgConsumerMessages.Done()
		for err := range consumer.Errors() {
			log.Fatalln("Error occured in Partition Consumer:", err)
		}
	}

	for index, partition := range partitions {
		pConsumer, err := consumer.ConsumePartition(ConsumerOffsetTopic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalln("Error occured while consuming partition.", err)
		}
		partitionsConsumers[index] = pConsumer
		qsm.wgConsumerMessages.Add(2)
		go getConsumerMessages(pConsumer)
		go getConsumerErrors(pConsumer)
	}

	qsm.wgConsumerMessages.Wait()
	for _, pConsumer := range partitionsConsumers {
		pConsumer.AsyncClose()
	}
}

// Burrow-based Consumer Offset Message parser function.
func (qsm *QueueSizeMonitor) formatConsumerOffsetMessage(message *sarama.ConsumerMessage) {

	defer qsm.wgConsumerMessages.Done()
	log.Println("Formatting Consumer Offset Message...")

	readString := func(buf *bytes.Buffer) (string, error) {
		var strlen uint16
		err := binary.Read(buf, binary.BigEndian, &strlen)
		if err != nil {
			return "", err
		}
		strbytes := make([]byte, strlen)
		n, err := buf.Read(strbytes)
		if (err != nil) || (n != int(strlen)) {
			return "", fmt.Errorf("string underflow")
		}
		return string(strbytes), nil
	}

	logError := func(err error) {
		log.Fatalln("Error while parsing message.", err)
	}

	var keyver, valver uint16
	var group, topic string
	var partition uint32
	var offset, timestamp uint64

	buf := bytes.NewBuffer(message.Key)
	err := binary.Read(buf, binary.BigEndian, &keyver)
	switch keyver {
	case 0, 1:
		group, err = readString(buf)
		if err != nil {
			logError(err)
			return
		}
		topic, err = readString(buf)
		if err != nil {
			logError(err)
			return
		}
		err = binary.Read(buf, binary.BigEndian, &partition)
		if err != nil {
			logError(err)
			return
		}
	case 2:
		logError(err)
		return
	default:
		logError(err)
		return
	}

	buf = bytes.NewBuffer(message.Value)
	err = binary.Read(buf, binary.BigEndian, &valver)
	if (err != nil) || ((valver != 0) && (valver != 1)) {
		logError(err)
		return
	}
	err = binary.Read(buf, binary.BigEndian, &offset)
	if err != nil {
		logError(err)
		return
	}
	_, err = readString(buf)
	if err != nil {
		logError(err)
		return
	}
	err = binary.Read(buf, binary.BigEndian, &timestamp)
	if err != nil {
		logError(err)
		return
	}

	partitionOffset := &PartitionOffset{
		Topic:     topic,
		Partition: int32(partition),
		Group:     group,
		Timestamp: int64(timestamp),
		Offset:    int64(offset),
	}

	log.Println("Consumer Offset from formatted message:", partitionOffset.Offset)
	qsm.ConsumerOffsetChannel <- partitionOffset
}
