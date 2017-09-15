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
	ConsumerOffsetStore       []*PartitionOffset
	ConsumerOffsetStoreMutex  sync.Mutex
	wgBrokerOffsetResponse    sync.WaitGroup
	BrokerOffsetStore         []*PartitionOffset
	BrokerOffsetStoreMutex    sync.Mutex
	StatsdClient              *statsd.StatsdClient
	StatsdCfg                 StatsdConfig
}

// NewQueueSizeMonitor : Returns a QueueSizeMonitor with an initialized client
// based on the comma-separated brokers (eg. "localhost:9092") along with 
// the Statsd instance address (eg. "localhost:8125").
func NewQueueSizeMonitor(brokers []string, statsdCfg StatsdConfig) (*QueueSizeMonitor, error) {
	
	config := sarama.NewConfig()
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}
	
	statsdClient := statsd.NewStatsdClient(statsdCfg.addr, statsdCfg.prefix)
	err = statsdClient.CreateSocket()
	if err != nil {
		return nil, err
	}
	
	qsm := &QueueSizeMonitor{}
	qsm.Client = client
	qsm.ConsumerOffsetStore = make([]*PartitionOffset, 0)
	qsm.BrokerOffsetStore = make([]*PartitionOffset, 0)
	qsm.StatsdClient = statsdClient
	qsm.StatsdCfg = statsdCfg
	return qsm, err
}

// Start : Initiates the monitoring procedure, prints out lag results.
func (qsm *QueueSizeMonitor) Start() {
	go qsm.GetConsumerOffsets()
	for {
		qsm.GetBrokerOffsets()
		brokerOffsetMap := qsm.createTPOffsetMap(qsm.BrokerOffsetStore, &qsm.BrokerOffsetStoreMutex)
		consumerOffsetMap := qsm.createGTPOffsetMap(qsm.ConsumerOffsetStore, &qsm.ConsumerOffsetStoreMutex)
		lagMap := qsm.generateLagMap(brokerOffsetMap, consumerOffsetMap)
		for group, gbody := range lagMap {
			for topic, tbody := range gbody {
				for partition, pbody := range tbody {
					stat := fmt.Sprintf("%s.group.%s.%s.%d", 
						qsm.StatsdCfg.prefix, group, topic, partition)
					go qsm.sendGaugeToStatsd(stat, pbody)
					log.Printf("Gauge sent to Statsd: %s=%d", stat, pbody)
				}
			}
		}
		time.Sleep(1 * time.Minute)
	}
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
			log.Fatalln("Error occured in Partition Consumer:", err)
		}
	}

	for index, partition := range partitions {
		pConsumer, err := consumer.ConsumePartition(ConsumerOffsetTopic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalln("Error occured while consuming partition.", err)
		}
		partitionsConsumers[index] = pConsumer
		log.Println("Partition Consumer Index:", index)
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
				log.Fatalln("Error occured while fetching leader broker.", err)
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
			log.Fatalln("Error while getting available offsets from broker.", err)
			request.Broker.Close()
			return
		}

		for topic, partitionMap := range response.Blocks {
			for partition, offsetResponseBlock := range partitionMap {
				if offsetResponseBlock.Err != sarama.ErrNoError {
					log.Fatalln("Error in offset response block.", 
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
func (qsm *QueueSizeMonitor) getTopicsAndPartitions(offsetStore []*PartitionOffset, mutex *sync.Mutex) map[string][]int32 {
	defer mutex.Unlock()
	mutex.Lock()
	tpMap := make(map[string][]int32)
	for _, partitionOffset := range offsetStore {
		topic, partition := partitionOffset.Topic, partitionOffset.Partition
		hasPartition := false
		for _, ele := range tpMap[topic] {
			if ele == partition {
				hasPartition = true
				break
			}
		}
		if !hasPartition {
			tpMap[topic] = append(tpMap[topic], partition)
		}
	}
	return tpMap
}

// Parses the Offset store and creates a group -> topic -> partition -> offset map.
func (qsm *QueueSizeMonitor) createGTPOffsetMap(offsetStore []*PartitionOffset,
	mutex *sync.Mutex) GTPOffsetMap {
	defer mutex.Unlock()
	mutex.Lock()
	gtpMap := make(GTPOffsetMap)
	for _, partitionOffset := range offsetStore {
		group, topic, partition, offset := partitionOffset.Group, partitionOffset.Topic,
			partitionOffset.Partition, partitionOffset.Offset
		if _, ok := gtpMap[group]; !ok {
			gtpMap[group] = make(TPOffsetMap)
		}
		if _, ok := gtpMap[group][topic]; !ok {
			gtpMap[group][topic] = make(POffsetMap)
		}
		gtpMap[group][topic][partition] = offset
	}
	return gtpMap
}

// Parses the Offset store and creates a topic -> partition -> offset map.
func (qsm *QueueSizeMonitor) createTPOffsetMap(offsetStore []*PartitionOffset,
	mutex *sync.Mutex) TPOffsetMap {
	defer mutex.Unlock()
	mutex.Lock()
	tpMap := make(TPOffsetMap)
	for _, partitionOffset := range offsetStore {
		topic, partition, offset := partitionOffset.Topic, partitionOffset.Partition, partitionOffset.Offset
		if _, ok := tpMap[topic]; !ok {
			tpMap[topic] = make(POffsetMap)
		}
		tpMap[topic][partition] = offset
	}
	return tpMap
}

// Generates a lag map from Broker and Consumer offset maps.
func (qsm *QueueSizeMonitor) generateLagMap(brokerOffsetMap TPOffsetMap, consumerOffsetMap GTPOffsetMap) GTPOffsetMap {
	lagMap := make(GTPOffsetMap)
	for group, gbody := range consumerOffsetMap {
		if _, ok := lagMap[group]; !ok {
			lagMap[group] = make(TPOffsetMap)
		}
		for topic, tbody := range gbody {
			if _, ok := lagMap[group][topic]; !ok {
				lagMap[group][topic] = make(POffsetMap)
			}
			for partition := range tbody {
				lagMap[group][topic][partition] = 
					brokerOffsetMap[topic][partition] - consumerOffsetMap[group][topic][partition]
				log.Println("\n+++++++++++++++++++++\nBroker Offset:", 
					brokerOffsetMap[topic][partition], 
					"\nConsumer Offset:", consumerOffsetMap[group][topic][partition],
					"\n+++++++++++++++++++++")
			}
		}
	}
	return lagMap
}

// Store newly received consumer offset.
func (qsm *QueueSizeMonitor) storeConsumerOffset(newOffset *PartitionOffset) {
	defer qsm.ConsumerOffsetStoreMutex.Unlock()
	qsm.ConsumerOffsetStoreMutex.Lock()
	qsm.ConsumerOffsetStore = append(qsm.ConsumerOffsetStore, newOffset)
}

// Store newly received broker offset.
func (qsm *QueueSizeMonitor) storeBrokerOffset(newOffset *PartitionOffset) {
	defer qsm.BrokerOffsetStoreMutex.Unlock()
	qsm.BrokerOffsetStoreMutex.Lock()
	qsm.BrokerOffsetStore = append(qsm.BrokerOffsetStore, newOffset)
}

// Sends the gauge to Statsd.
func (qsm *QueueSizeMonitor) sendGaugeToStatsd(stat string, value int64) {
	if qsm.StatsdClient == nil {
		log.Fatalln("Statsd Client not initialized yet.")
		return
	}
	err := qsm.StatsdClient.Gauge(stat, value)
	if err != nil {
		log.Fatalln("Error while sending gauge to statsd:", err)
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

	log.Println("Consumer Offset from message:", partitionOffset.Offset)
	qsm.storeConsumerOffset(partitionOffset)
}
