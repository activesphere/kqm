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

// Start : Initiates the monitoring procedure, prints out the lag results
// and sends the results to Statsd.
func Start(cfg *QSMConfig) {
	var (
		qsm *QueueSizeMonitor
		err error
	)

	for {
		qsm, err = NewQueueSizeMonitor(cfg)
		if err != nil {
			log.Println("Error while creating QSM instance.", err)
			time.Sleep(cfg.ReadInterval)
			continue
		}
		break
	}

	go qsm.GetConsumerOffsets()
	for {
		qsm.GetBrokerOffsets()
		qsm.computeLag(qsm.BrokerOffsetStore, qsm.ConsumerOffsetStore)
		time.Sleep(cfg.ReadInterval)
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

// RetryOnFailure : As the name suggests, it retries the func passed an argument
// based on the Max Retries and Retry Interval specified in the config.
func (qsm *QueueSizeMonitor) RetryOnFailure(title string, fn func() (interface{}, error)) (interface{}, error) {
	var (
		count  int
		result interface{}
		err    error
	)
	for count := 1; count <= qsm.Config.MaxRetries; count++ {
		result, err = fn()
		if err != nil {
			log.Println("Retrying due to error:", title)
			time.Sleep(qsm.Config.RetryInterval)
			continue
		}
		break
	}
	if count > qsm.Config.MaxRetries {
		log.Println("Max Retries Exceeded: ", title)
		return result, err
	}
	log.Println("Retry Succeeded: ", title)
	return result, nil
}

// GetConsumerOffsets : Subcribes to Offset Topic and parses messages to 
// obtains Consumer Offsets.
func (qsm *QueueSizeMonitor) GetConsumerOffsets() {
	log.Println("Started getting consumer partition offsets...")
	
	result, err := qsm.RetryOnFailure("Fetch_Client_Partitions", func() (interface{}, error) {
		return qsm.Client.Partitions(ConsumerOffsetTopic)
	})
	if err != nil {
		log.Println("Error occured while getting client partitions.", err)
		return
	}
	partitions := result.([]int32)

	result, err = qsm.RetryOnFailure("Fetch_Client_Consumer", func() (interface{}, error) {
		return sarama.NewConsumerFromClient(qsm.Client)
	})
	if err != nil {
		log.Println("Error occured while creating new client consumer.", err)
		return
	}
	consumer := result.(sarama.Consumer)

	partitionsConsumers := make([]sarama.PartitionConsumer, len(partitions))
	log.Println("Number of Partition Consumers:", len(partitions))

	getConsumerMessages := func(consumer sarama.PartitionConsumer) {
		defer qsm.wgConsumerMessages.Done()
		for message := range consumer.Messages() {
			qsm.wgConsumerMessages.Add(1)
			go qsm.formatConsumerOffsetMessage(message)
		}
	}

	getConsumerErrors := func(consumer sarama.PartitionConsumer) {
		defer qsm.wgConsumerMessages.Done()
		for err := range consumer.Errors() {
			log.Println("Error occured in Partition Consumer:", err)
		}
	}

	for index, partition := range partitions {
		pConsumer, err := consumer.ConsumePartition(ConsumerOffsetTopic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Println("Error occured while consuming partition.", err)
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

// GetBrokerOffsets : Finds out the leader brokers for the partitions and 
// gets the latest commited offsets.
func (qsm *QueueSizeMonitor) GetBrokerOffsets() {
	
	tpMap := qsm.getTopicsAndPartitions(qsm.ConsumerOffsetStore, &qsm.ConsumerOffsetStoreMutex)
	brokerOffsetRequests := make(map[int32]BrokerOffsetRequest)

	for topic, partitions := range tpMap {
		for _, partition := range partitions {
			leaderBroker, err := qsm.Client.Leader(topic, partition)
			if err != nil {
				log.Println("Error occured while fetching leader broker.", err)
				continue
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
	
	getOffsetResponse := func(request *BrokerOffsetRequest) {
		defer qsm.wgBrokerOffsetResponse.Done()
		response, err := request.Broker.GetAvailableOffsets(request.OffsetRequest)
		if err != nil {
			log.Println("Error while getting available offsets from broker.", err)
			request.Broker.Close()
			return
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
	}

	for _, brokerOffsetRequest := range brokerOffsetRequests {
		qsm.wgBrokerOffsetResponse.Add(1)
		go getOffsetResponse(&brokerOffsetRequest)
	}
	qsm.wgBrokerOffsetResponse.Wait()
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
func (qsm *QueueSizeMonitor) computeLag(brokerOffsetMap TPOffsetMap, consumerOffsetMap GTPOffsetMap) {
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
				go qsm.sendGaugeToStatsd(stat, lag)
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
func (qsm *QueueSizeMonitor) sendGaugeToStatsd(stat string, value int64) {
	for {
		if qsm.StatsdClient == nil {
			log.Println("Statsd Client not initialized yet.")
			time.Sleep(qsm.Config.RetryInterval)
			continue
		}
		err := qsm.StatsdClient.Gauge(stat, value)
		if err != nil {
			log.Println("Error while sending gauge to statsd:", err)
			time.Sleep(qsm.Config.RetryInterval)
			continue
		}
		log.Printf("Gauge sent to Statsd: %s=%d", stat, value)
		break
	}
}

// Burrow-based Consumer Offset Message parser function.
func (qsm *QueueSizeMonitor) formatConsumerOffsetMessage(message *sarama.ConsumerMessage) {	
	defer qsm.wgConsumerMessages.Done()

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
		log.Println("Error while parsing message.", err)
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

	qsm.storeConsumerOffset(partitionOffset)
}
