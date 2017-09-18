package main

import (
	"time"
	"fmt"
	"sync"
	"bytes"
	"encoding/binary"
	"log"
	"github.com/Shopify/sarama"
	"github.com/quipo/statsd"
)

// ConsumerOffsetTopic : provides the topic name of the Offset Topic.
const ConsumerOffsetTopic = "__consumer_offsets"

// QueueSizeMonitor : Defines the type for Kafka Queue Size 
// Monitor implementation using Sarama.
type QueueSizeMonitor struct {
	Client                    sarama.Client
	wgConsumerMessages        sync.WaitGroup
	ConsumerOffsetStore       GTPOffsetMap
	ConsumerOffsetStoreMutex  sync.Mutex
	wgBrokerOffsetResponse    sync.WaitGroup
	BrokerOffsetStore         TPOffsetMap
	BrokerOffsetStoreMutex    sync.Mutex
	StatsdClient              *statsd.StatsdClient
	Config                    *QSMConfig
}

// RetryOnFailure : As the name suggests, it retries the func passed an argument
// based on the Max Retries and Retry Interval specified in the config.
func RetryOnFailure(cfg *QSMConfig, title string, 
	fn func() error) error {
	var (
		count  int
		err    error
	)
	for count := 1; count <= cfg.MaxRetries; count++ {
		err = fn()
		if err != nil {
			log.Println("Retrying due to error:", title)
			time.Sleep(cfg.RetryInterval)
			continue
		}
		break
	}
	if count > cfg.MaxRetries {
		log.Println("Max Retries Exceeded: ", title)
		return err
	}
	log.Println("Succeeded: ", title)
	return nil
}

// Start : Initiates the monitoring procedure, prints out the lag results
// and sends the results to Statsd.
func Start(cfg *QSMConfig) {
	qsm, err := NewQueueSizeMonitor(cfg)
	if err != nil {
		log.Println("Error while creating QSM instance.", err)
		return
	}

	go func() {
		RetryOnFailure(cfg, "CONSUMER_OFFSETS", func() error {
			return qsm.GetConsumerOffsets()
		})
	}()

	for {
		RetryOnFailure(cfg, "REPORT_LAG", func() error {
			err := qsm.GetBrokerOffsets()
			if err != nil {
				return err
			}
			err = qsm.computeLag(qsm.BrokerOffsetStore, qsm.ConsumerOffsetStore)
			if err != nil {
				return err
			}
			time.Sleep(cfg.ReadInterval)
			return nil
		})
	}
}

// NewQueueSizeMonitor : Returns a QueueSizeMonitor with an initialized client
// based on the comma-separated brokers (eg. "localhost:9092") along with 
// the Statsd instance address (eg. "localhost:8125").
func NewQueueSizeMonitor(cfg *QSMConfig) (*QueueSizeMonitor, error) {
	
	config := sarama.NewConfig()
	client, err := sarama.NewClient(cfg.KafkaCfg.Brokers, config)
	if err != nil {
		return nil, err
	}
	
	statsdClient := statsd.NewStatsdClient(cfg.StatsdCfg.Addr, cfg.StatsdCfg.Prefix)
	err = statsdClient.CreateSocket()
	if err != nil {
		return nil, err
	}
	
	qsm := &QueueSizeMonitor{}
	qsm.Client = client
	qsm.ConsumerOffsetStore = make(GTPOffsetMap)
	qsm.BrokerOffsetStore = make(TPOffsetMap)
	qsm.StatsdClient = statsdClient
	qsm.Config = cfg
	return qsm, err
}

// GetConsumerOffsets : Subcribes to Offset Topic and parses messages to 
// obtains Consumer Offsets.
func (qsm *QueueSizeMonitor) GetConsumerOffsets() error {
	log.Println("Started getting consumer partition offsets...")
	
	partitions, err := qsm.Client.Partitions(ConsumerOffsetTopic)
	if err != nil {
		log.Println("Error occured while getting client partitions.", err)
		return err
	}

	consumer, err := sarama.NewConsumerFromClient(qsm.Client)
	if err != nil {
		log.Println("Error occured while creating new client consumer.", err)
		return err
	}

	log.Println("Number of Partition Consumers:", len(partitions))

	// Burrow-based Consumer Offset Message parser function.
	formatAndStoreMessage := func (message *sarama.ConsumerMessage) error {	
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

		var (
			keyver, valver uint16
			group, topic string
			partition uint32
			offset, timestamp uint64
		)

		buf := bytes.NewBuffer(message.Key)
		err := binary.Read(buf, binary.BigEndian, &keyver)
		switch keyver {
		case 0, 1:
			group, err = readString(buf)
			if err != nil {
				return err
			}
			topic, err = readString(buf)
			if err != nil {
				return err
			}
			err = binary.Read(buf, binary.BigEndian, &partition)
			if err != nil {
				return err
			}
		case 2:
			return err
		default:
			return err
		}

		buf = bytes.NewBuffer(message.Value)
		err = binary.Read(buf, binary.BigEndian, &valver)
		if (err != nil) || ((valver != 0) && (valver != 1)) {
			return err
		}
		err = binary.Read(buf, binary.BigEndian, &offset)
		if err != nil {
			return err
		}
		_, err = readString(buf)
		if err != nil {
			return err
		}
		err = binary.Read(buf, binary.BigEndian, &timestamp)
		if err != nil {
			return err
		}

		partitionOffset := &PartitionOffset{
			Topic:     topic,
			Partition: int32(partition),
			Group:     group,
			Timestamp: int64(timestamp),
			Offset:    int64(offset),
		}

		qsm.storeConsumerOffset(partitionOffset)
		log.Println("Consumer Offset: ", partitionOffset.Offset)
		return nil
	}

	storeMessages := func(partition int32) error {
		pConsumer, err := consumer.ConsumePartition(ConsumerOffsetTopic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Println("Error occured while consuming partition.", err)
			pConsumer.AsyncClose()
			return err
		}
		for message := range pConsumer.Messages() {
			go formatAndStoreMessage(message)
		}
		return nil
	}

	for _, partition := range partitions {
		err := storeMessages(partition)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetBrokerOffsets : Finds out the leader brokers for the partitions and 
// gets the latest commited offsets.
func (qsm *QueueSizeMonitor) GetBrokerOffsets() error {
	
	tpMap := qsm.getTopicsAndPartitions(qsm.ConsumerOffsetStore, &qsm.ConsumerOffsetStoreMutex)
	brokerOffsetRequests := make(map[int32]BrokerOffsetRequest)

	for topic, partitions := range tpMap {
		for _, partition := range partitions {
			leaderBroker, err := qsm.Client.Leader(topic, partition)
			if err != nil {
				log.Println("Error occured while fetching leader broker.", err)
				return err
			}
			leaderBrokerID := leaderBroker.ID()

			if _, ok := brokerOffsetRequests[leaderBrokerID]; !ok {
				brokerOffsetRequests[leaderBrokerID] = BrokerOffsetRequest{
					Broker: leaderBroker,
					OffsetRequest: &sarama.OffsetRequest{},
				}
			} else {
				brokerOffsetRequests[leaderBrokerID].OffsetRequest.
					AddBlock(topic, partition, sarama.OffsetNewest, 1)
			}
		}
	}
	
	storeResponse := func(request *BrokerOffsetRequest) error {
		response, err := request.Broker.GetAvailableOffsets(request.OffsetRequest)
		if err != nil {
			log.Println("Error while getting available offsets from broker.", err)
			return err
		}

		for topic, partitionMap := range response.Blocks {
			for partition, offsetResponseBlock := range partitionMap {
				if offsetResponseBlock.Err != sarama.ErrNoError {
					log.Println("Error in offset response block.", 
						offsetResponseBlock.Err.Error())
					continue
				}
				brokerOffset := &PartitionOffset{
					Topic: topic,
					Partition: partition,
					Offset: offsetResponseBlock.Offsets[0], // Version 0
					Timestamp: offsetResponseBlock.Timestamp,
				}
				qsm.storeBrokerOffset(brokerOffset)
			}
		}
		return nil
	}

	for _, brokerOffsetRequest := range brokerOffsetRequests {
		err := storeResponse(&brokerOffsetRequest)
		if err != nil {
			return err
		}
	}
	return nil
}

// Fetches topics and their corresponding partitions.
func (qsm *QueueSizeMonitor) getTopicsAndPartitions(offsetStore GTPOffsetMap, mutex *sync.Mutex) map[string][]int32 {
	defer mutex.Unlock()
	mutex.Lock()
	tpMap := make(map[string][]int32)
	for _, gbody := range offsetStore {
		for topic, tbody := range gbody {
			for partition := range tbody {
				tpMap[topic] = append(tpMap[topic], partition)
			}
		}
	}
	return tpMap
}

// Computes the lag and sends the data as a gauge to Statsd.
func (qsm *QueueSizeMonitor) computeLag(brokerOffsetMap TPOffsetMap, consumerOffsetMap GTPOffsetMap) error {
	for group, gbody := range consumerOffsetMap {
		for topic, tbody := range gbody {
			for partition := range tbody {
				lag := brokerOffsetMap[topic][partition] - consumerOffsetMap[group][topic][partition]
				stat := fmt.Sprintf("%s.group.%s.%s.%d", 
					qsm.Config.StatsdCfg.Prefix, group, topic, partition)
				if lag < 0 {
					log.Printf("Negative Lag received for %s: %d", stat, lag)
					continue
				}
				err := qsm.sendGaugeToStatsd(stat, lag)
				if err != nil {
					return err
				}
				log.Printf("\n+++++++++(Topic: %s, Partn: %d)++++++++++++" +
					"\nBroker Offset: %d" +
					"\nConsumer Offset: %d" +
					"\nLag: %d" +
					"\n++++++++++(Group: %s)+++++++++++", 
					topic, partition, brokerOffsetMap[topic][partition], 
					consumerOffsetMap[group][topic][partition], lag, group)
			}
		}
	}
	return nil
}

// Store newly received consumer offset.
func (qsm *QueueSizeMonitor) storeConsumerOffset(newOffset *PartitionOffset) {
	defer qsm.ConsumerOffsetStoreMutex.Unlock()
	qsm.ConsumerOffsetStoreMutex.Lock()
	group, topic, partition, offset := newOffset.Group, newOffset.Topic,
		newOffset.Partition, newOffset.Offset
	if _, ok := qsm.ConsumerOffsetStore[group]; !ok {
		qsm.ConsumerOffsetStore[group] = make(TPOffsetMap)
	}
	if _, ok := qsm.ConsumerOffsetStore[group][topic]; !ok {
		qsm.ConsumerOffsetStore[group][topic] = make(POffsetMap)
	}
	qsm.ConsumerOffsetStore[group][topic][partition] = offset
}

// Store newly received broker offset.
func (qsm *QueueSizeMonitor) storeBrokerOffset(newOffset *PartitionOffset) {
	defer qsm.BrokerOffsetStoreMutex.Unlock()
	qsm.BrokerOffsetStoreMutex.Lock()
	topic, partition, offset := newOffset.Topic, newOffset.Partition, newOffset.Offset
	if _, ok := qsm.BrokerOffsetStore[topic]; !ok {
		qsm.BrokerOffsetStore[topic] = make(POffsetMap)
	}
	qsm.BrokerOffsetStore[topic][partition] = offset
}

// Sends the gauge to Statsd.
func (qsm *QueueSizeMonitor) sendGaugeToStatsd(stat string, value int64) error {
	if qsm.StatsdClient == nil {
		message := "Statsd Client not initialized yet."
		log.Println(message)
		return fmt.Errorf(message)
	}
	err := qsm.StatsdClient.Gauge(stat, value)
	if err != nil {
		log.Println("Error while sending gauge to statsd:", err)
		return err
	}
	log.Printf("Gauge sent to Statsd: %s=%d", stat, value)
	return nil
}

