package tests

/*
	This is a UDP server implementation to mimick Statsd for testing purpose.
	This server will listen to the port passed as command line arg and print
	the result to stdout.
*/

import (
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/activesphere/kqm/monitor"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func toPartitionOffset(message *kafka.Message) *monitor.PartitionOffset {
	return &monitor.PartitionOffset{
		Topic:     *message.TopicPartition.Topic,
		Partition: message.TopicPartition.Partition,
		Offset:    int64(message.TopicPartition.Offset),
	}
}

func parseGauge(gauge string) (*monitor.PartitionOffset, error) {
	partOff := monitor.PartitionOffset{}

	var props []string

	props = strings.Split(gauge, ".")
	partOff.Group, partOff.Topic = props[2], props[3]

	props = strings.Split(strings.Trim(props[4], "|g"), ":")

	partition, err := strconv.Atoi(props[0])
	if err != nil {
		log.Errorln("Conversion from string to int failed for partition.")
		return nil, err
	}
	partOff.Partition = int32(partition)

	lag, err := strconv.Atoi(props[1])
	if err != nil {
		log.Errorln("Conversion from string to int failed for lag.")
		return nil, err
	}
	partOff.Offset = int64(lag)

	return &partOff, nil
}

func createProducer(broker string) (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
	})
	if err != nil {
		return nil, err
	}
	return producer, nil
}

func producerEvents(producer *kafka.Producer, doneChan chan *kafka.Message) {
	for e := range producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			m := ev
			if m.TopicPartition.Error != nil {
				log.Errorf("Delivery failed: %v", m.TopicPartition.Error)
				doneChan <- nil
				return
			}
			log.Debugf("Delivered message to topic %s [%d] at offset %v",
				*m.TopicPartition.Topic, m.TopicPartition.Partition,
				m.TopicPartition.Offset)
			doneChan <- ev
			return
		default:
			log.Debugf("Ignored event: %s", ev)
		}
	}
	doneChan <- nil
}

func produceMessage(producer *kafka.Producer, topic string,
	partition int32, value string) *kafka.Message {

	doneChan := make(chan *kafka.Message)
	defer close(doneChan)
	go producerEvents(producer, doneChan)

	producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
		},
		Value: []byte(value + "|" + time.Now().String()),
	}
	result := <-doneChan
	return result
}

func consumerEvents(consumer *kafka.Consumer) (*kafka.Message, error) {
	for {
		select {
		case event := <-consumer.Events():
			switch eventType := event.(type) {
			case kafka.AssignedPartitions:
				consumer.Assign(eventType.Partitions)
			case kafka.RevokedPartitions:
				consumer.Unassign()
			case *kafka.Message:
				return eventType, nil
			case kafka.PartitionEOF:
				log.Debugf("Reached %v", event)
			case kafka.Error:
				return nil, eventType
			}
		}
	}
}

func createConsumer(broker string, groupID string,
	topics []string) (*kafka.Consumer, error) {

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        groupID,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"}})

	if err != nil {
		return nil, err
	}

	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func equalPartitionOffsets(p1, p2 *monitor.PartitionOffset) bool {
	if p1.Topic == p2.Topic &&
		p1.Partition == p2.Partition &&
		p1.Group == p2.Group {
		return true
	}
	return false
}

func getConsumerLag(conn *net.UDPConn, srcPartOff *monitor.PartitionOffset) int64 {
	log.Infoln("Wait for 15 seconds for the updates to reflect in KQM.")
	time.Sleep(15 * time.Second)

	buffer := make([]byte, 512)
	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Errorln("Error reading from UDP: ", err)
			continue
		}

		recvPartOff, err := parseGauge(string(buffer[:n]))
		if err != nil {
			os.Exit(1)
		}

		if equalPartitionOffsets(srcPartOff, recvPartOff) {
			return recvPartOff.Offset
		}
	}
}

// TestLag : Basic test for Lag.
func TestLag(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	serverAddr, err := net.ResolveUDPAddr("udp", ":8125")
	if err != nil {
		log.Fatalln("Error in resolving Addr:", err)
	}
	conn, err := net.ListenUDP("udp", serverAddr)
	if err != nil {
		log.Fatalln("Error while listening to UDP port.")
	}
	defer conn.Close()

	const (
		broker        = "localhost:9092"
		topicPrefix   = "topic"
		groupIDPrefix = "clark-kent-"
		partition     = 0
	)

	producer, err := createProducer(broker)
	if err != nil {
		log.Fatalln("Error while creating Producer.")
	}
	defer producer.Close()

	produceMessages := func(topic string, num int) {
		for i := 1; i <= num; i++ {
			message := produceMessage(producer, topic, partition,
				"MSG"+strconv.Itoa(i))
			if message == nil {
				log.Fatalln("There was a problem in producing the message.")
			}
			producedPartOff := toPartitionOffset(message)
			log.Infof("Produced Message on topic: %s, partn: %d.",
				producedPartOff.Topic, producedPartOff.Partition)
		}
	}

	consumeMessages := func(topic string, groupID string, numMessages int) {
		consumer, err := createConsumer(broker, groupID, []string{topic})
		if err != nil {
			log.Fatalln("Error while creating Consumer.")
		}

		log.Infoln("Consuming Messages from Topic:", topic)
		for i := 1; i <= numMessages; i++ {
			message, err := consumerEvents(consumer)
			if err != nil {
				log.Fatalln("There was a problem while consuming message.", err)
			}
			log.Infof("Consumer Received Message on Topic: %s, Partn: "+
				"%d, Message: %s", *message.TopicPartition.Topic,
				message.TopicPartition.Partition, message.Value)
		}

		log.Infoln("Closing the Consumer.")
		consumer.Close()
	}

	checkLag := func(topic string, groupID string, messageCount int) {

		log.Printf(`
			##################################################################
			Start the consumer so that KQM becomes aware of the new consumer.
			Check the lag, it should be zero since there are no messages
			produced yet.
			##################################################################
		`)
		consumeMessages(topic, groupID, 0)

		lag := getConsumerLag(conn, &monitor.PartitionOffset{
			Topic:     topic,
			Partition: partition,
			Group:     groupID,
		})
		log.Infof("Lag at (Group: %s, Topic: %s, Partn: %d): %d",
			groupID, topic, partition, lag)
		assert.Equal(t, int64(0), lag)

		log.Printf(`
			##################################################################
			Produce %d messages and check for the lag. It should be %d since
			the consumer hasn't consumed those messages yet.
			##################################################################
		`, messageCount, messageCount)
		produceMessages(topic, messageCount)

		lag = getConsumerLag(conn, &monitor.PartitionOffset{
			Topic:     topic,
			Partition: partition,
			Group:     groupID,
		})
		log.Infof("Lag at (Topic: %s, Partn: %d): %d", topic, partition, lag)
		assert.Equal(t, int64(messageCount), lag)

		log.Printf(`
			##################################################################
			Initiate the consumption of messages and check for the lag once
			again. It should be zero this time since the consumer will have
			consumed the messages.
			##################################################################
		`)
		consumeMessages(topic, groupID, messageCount)

		lag = getConsumerLag(conn, &monitor.PartitionOffset{
			Topic:     topic,
			Partition: partition,
			Group:     groupID,
		})
		log.Infof("Lag at (Topic: %s, Partn: %d): %d", topic, partition, lag)
		assert.Equal(t, int64(0), lag)
	}

	// Check from 10 to 100 messages.
	for i := 1; i <= 2; i++ {
		scale := int(math.Pow10(i))
		log.Printf(`
			******************************************************************
			# Lag Validation for scale: %s									 #
			******************************************************************
		`, strconv.Itoa(scale))
		index := strconv.Itoa(i)
		topic := topicPrefix + index
		groupID := groupIDPrefix + index
		checkLag(topic, groupID, scale)
	}
}
