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
				log.Debugln("Consumer: AssignedPartitions event received.")
				consumer.Assign(eventType.Partitions)
			case kafka.RevokedPartitions:
				log.Debugln("Consumer: RevokedPartitions event received.")
				consumer.Unassign()
			case *kafka.Message:
				log.Debugln("Consumer: Message event received.")
				return eventType, nil
			case kafka.PartitionEOF:
				log.Debugf("Consumer: PartitionEOF event received. Reached %v",
					event)
			case kafka.Error:
				log.Debugln("Consumer: ERROR event received.")
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
		"go.events.channel.size":          1,
		"go.application.rebalance.enable": false,
		"enable.auto.commit":              false,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"},
	})

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
	log.Debugln("Getting consumer lag from statsd-mimicking UDP server.")
	buffer := make([]byte, 512)
	for {
		log.Debugln("UDP server is reading from UDP port 8125.")
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Errorln("Error reading from UDP: ", err)
			continue
		}

		recvData := string(buffer[:n])
		log.Debugf("UDP Server received data: %s", recvData)

		recvPartOff, err := parseGauge(recvData)
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
	log.SetLevel(log.InfoLevel)

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
			log.Debugf("Produced Message on topic: %s, partn: %d.",
				producedPartOff.Topic, producedPartOff.Partition)
		}
		log.Infof("Produced %d Messages on topic: %s.", num, topic)
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
			_, err = consumer.CommitMessage(message)
			if err != nil {
				log.Debugf("There was a problem while committing message: %s",
					message.Value)
			} else {
				log.Debugf("Consumer Received Message on Topic: %s, Partn: "+
					"%d, Message: %s", *message.TopicPartition.Topic,
					message.TopicPartition.Partition, message.Value)
			}
		}
		log.Infof("Consumer with group %s received %d messages on topic %s.",
			groupID, numMessages, topic)

		log.Debugln("Unassigning Partition from the Consumer.")
		consumer.Unassign()

		log.Infoln("Closing the Consumer.")
		err = consumer.Close()
		if err != nil {
			log.Debugf("There was a problem while closing the consumer with "+
				"GroupID: %s, Topic: %s, MessageCount: %d", groupID,
				topic, numMessages)
		} else {
			log.Debugf("Messages consumed successfully for "+
				"GroupID: %s, Topic: %s, MessageCount: %d", groupID,
				topic, numMessages)
		}
	}

	checkLag := func(topic string, groupID string, messageCount int) {

		log.Printf(`
			##################################################################
			Produce a message and start the consumer to consume it so that
			KQM becomes aware of the new consumer. Check the lag, it should
			be zero since the message produced has already been consumed.
			##################################################################
		`)
		produceMessages(topic, 1)
		consumeMessages(topic, groupID, 1)

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
			Produce %d messages and consume them using the same consumer.
			Check the lag for the consumer, it should be zero since the
			consumer will consume all the produced messages.
			##################################################################
		`, messageCount)
		produceMessages(topic, messageCount)
		consumeMessages(topic, groupID, messageCount)

		waitInSeconds := 5
		for waitInSeconds <= 60 {
			log.Infof("Waiting for %d seconds for the updates to reflect "+
				"in KQM.", waitInSeconds)
			time.Sleep(time.Duration(waitInSeconds) * time.Second)

			lag = getConsumerLag(conn, &monitor.PartitionOffset{
				Topic:     topic,
				Partition: partition,
				Group:     groupID,
			})
			log.Infof("Lag at (Group: %s, Topic: %s, Partn: %d): %d", groupID,
				topic, partition, lag)

			if lag == 0 {
				return
			}

			log.Infof("Non-zero lag obtained, retrying with a greater " +
				"sleep time.")
			waitInSeconds += 5
		}
		assert.FailNow(t, "FAILURE. Non-zero lag even after multiple retries.")
	}

	// Check from 10 to 10000 messages.
	for i := 1; i <= 4; i++ {
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
