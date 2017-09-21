package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/quipo/statsd"
	"golang.org/x/sync/syncmap"
)

// ConsumerOffsetTopic : provides the topic name of the Offset Topic.
const ConsumerOffsetTopic = "__consumer_offsets"

// Retry : It retries the func passed an argument based on the whether or not
// the the fn returns an error.
func Retry(cfg *QMConfig, title string, fn func() error) {
	for {
		err := fn()
		if err != nil {
			log.Println("Retrying due to a sychronous error:", title)
			time.Sleep(cfg.ReadInterval)
			continue
		}
		log.Println("Completed Execution Successfully:", title)
		break
	}
}

// RetryWithChannel : It retries the func passed an argument
// based on the whether the the fn returns an error or the
// Error channel receives an error or not.
func RetryWithChannel(cfg *QMConfig, title string, fn func(ec chan error) error) {
	errorChannel := make(chan error)
	var err error
	for {
		err = fn(errorChannel)
		if err != nil {
			log.Println("Retrying due to a error returned by fn:", title)
			time.Sleep(cfg.ReadInterval)
			continue
		}
		err = <-errorChannel
		if err != nil {
			log.Println("Retrying due to a error received from channel:", title)
			time.Sleep(cfg.ReadInterval)
			continue
		}
		log.Println("Completed Execution Successfully:", title)
		break
	}
}

// Start : Initiates the monitoring procedure, prints out the lag results
// and sends the results to Statsd.
func Start(cfg *QMConfig) {
	qm, err := NewQueueMonitor(cfg)
	if err != nil {
		log.Println("Error while creating QueueMonitor instance.", err)
		return
	}

	go func() {
		RetryWithChannel(cfg, "CONSUMER_OFFSETS", func(ec chan error) error {
			return qm.GetConsumerOffsets(ec)
		})
	}()

	for {
		Retry(cfg, "REPORT_LAG", func() error {
			err := qm.GetBrokerOffsets()
			if err != nil {
				return err
			}
			time.Sleep(cfg.ReadInterval)
			return nil
		})
	}
}

// NewQueueMonitor : Returns a QueueMonitor with an initialized client
// based on the comma-separated brokers (eg. "localhost:9092") along with
// the Statsd instance address (eg. "localhost:8125").
func NewQueueMonitor(cfg *QMConfig) (*QueueMonitor, error) {

	config := sarama.NewConfig()
	// Setting Consumer.Return.Errors to true enables sending the Partition
	// Consumer Errors to the Error Channel instead of logging them.
	config.Consumer.Return.Errors = true
	client, err := sarama.NewClient(cfg.KafkaCfg.Brokers, config)
	if err != nil {
		return nil, err
	}

	qm := &QueueMonitor{}
	qm.Client = client
	qm.OffsetStore = new(syncmap.Map)
	qm.Config = cfg

	statsdClient := statsd.NewStatsdClient(cfg.StatsdCfg.Addr,
		cfg.StatsdCfg.Prefix)
	err = statsdClient.CreateSocket()
	if err != nil {
		return nil, err
	}
	qm.StatsdClient = statsdClient
	return qm, err
}

// GetConsumerOffsets : Subcribes to Offset Topic and parses messages to
// obtains Consumer Offsets.
func (qm *QueueMonitor) GetConsumerOffsets(errorChannel chan error) error {
	log.Println("Started getting consumer partition offsets...")

	consumeMessage := func(pConsumers *PartitionConsumers, index int) {
		defer pConsumers.AsyncCloseAll()
		for message := range pConsumers.Handles[index].Messages() {
			partitionOffset, err := formatConsumerMessage(message)
			if err != nil {
				log.Println("Error while parsing consumer message:", err)
				continue
			}
			if partitionOffset != nil {
				qm.storeConsumerOffset(partitionOffset)
			}
		}
	}

	checkErrors := func(pConsumers *PartitionConsumers, index int) {
		defer pConsumers.AsyncCloseAll()
		consumerError := <-pConsumers.Handles[index].Errors()
		if consumerError != nil {
			errorChannel <- consumerError.Err
		}
	}

	partitions, err := qm.Client.Partitions(ConsumerOffsetTopic)
	if err != nil {
		log.Println("Error occured while getting client partitions.", err)
		return err
	}

	consumer, err := sarama.NewConsumerFromClient(qm.Client)
	if err != nil {
		log.Println("Error occured while creating new client consumer.", err)
		return err
	}

	partitionConsumers := PartitionConsumers{
		Handles:   make([]sarama.PartitionConsumer, 0),
		mutex:     &sync.Mutex{},
		areClosed: false,
	}
	defer partitionConsumers.AsyncCloseAll()

	for _, partition := range partitions {
		pConsumer, err := consumer.ConsumePartition(ConsumerOffsetTopic,
			partition, sarama.OffsetOldest)
		if err != nil {
			log.Println("Error occured while consuming partition.", err)
			return err
		}
		partitionConsumers.Add(pConsumer)
	}

	for index := range partitionConsumers.Handles {
		go consumeMessage(&partitionConsumers, index)
		go checkErrors(&partitionConsumers, index)
	}

	return nil
}

// GetBrokerOffsets : Finds out the leader brokers for the partitions and
// gets the latest commited offsets.
func (qm *QueueMonitor) GetBrokerOffsets() error {

	tpMap := qm.getTopicsAndPartitions(qm.OffsetStore)
	brokerOffsetRequests := make(map[int32]BrokerOffsetRequest)

	for topic, partitions := range tpMap {
		for _, partition := range partitions {
			leaderBroker, err := qm.Client.Leader(topic, partition)
			if err != nil {
				log.Println("Error occured while fetching leader broker.", err)
				return err
			}
			leaderBrokerID := leaderBroker.ID()

			if _, ok := brokerOffsetRequests[leaderBrokerID]; !ok {
				brokerOffsetRequests[leaderBrokerID] = BrokerOffsetRequest{
					Broker:        leaderBroker,
					OffsetRequest: &sarama.OffsetRequest{},
				}
			} else {
				brokerOffsetRequests[leaderBrokerID].OffsetRequest.
					AddBlock(topic, partition, sarama.OffsetNewest, 1)
			}
		}
	}

	sendBrokerOffsets := func(request *BrokerOffsetRequest) error {
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
				brokerOffset := offsetResponseBlock.Offsets[0]
				qm.lag(topic, partition, brokerOffset)
			}
		}
		return nil
	}

	for _, brokerOffsetRequest := range brokerOffsetRequests {
		err := sendBrokerOffsets(&brokerOffsetRequest)
		if err != nil {
			return err
		}
	}
	return nil
}

// Fetches topics and their corresponding partitions.
func (qm *QueueMonitor) getTopicsAndPartitions(offsetStore *syncmap.Map) map[string][]int32 {
	tpMap := make(map[string][]int32)
	offsetStore.Range(func(topicI, tbodyI interface{}) bool {
		topic := topicI.(string)
		tbodyI.(*syncmap.Map).Range(func(partitionI, _ interface{}) bool {
			tpMap[topic] = append(tpMap[topic], partitionI.(int32))
			return true
		})
		return true
	})
	return tpMap
}

// Computes the lag and sends the data as a gauge to Statsd.
func (qm *QueueMonitor) lag(topic string, partition int32, brokerOffset int64) error {
	tmp, ok := qm.OffsetStore.Load(topic)
	if !ok {
		return fmt.Errorf("Topic doesn't exist in Offset Store: %s", topic)
	}
	pOffsetMap, ok := tmp.(*syncmap.Map)
	if !ok {
		return fmt.Errorf("Not a valid syncmap at Topic: %s", topic)
	}
	pOffsetMap.Range(func(groupI, offsetI interface{}) bool {
		group, ok := groupI.(string)
		if !ok {
			log.Println("Invalid cast to string for group.")
			return false
		}
		offset, ok := offsetI.(int64)
		if !ok {
			log.Println("Invalid cast to int64 for offset.")
			return false
		}
		lag := brokerOffset - offset
		stat := fmt.Sprintf("%s.group.%s.%s.%d", qm.Config.StatsdCfg.Prefix,
			group, topic, partition)
		if lag < 0 {
			log.Printf("Negative Lag received for %s: %d", stat, lag)
		} else {
			go qm.sendGaugeToStatsd(stat, lag)
		}
		return true
	})
	return nil
}

// Store newly received consumer offset.
func (qm *QueueMonitor) storeConsumerOffset(newOffset *PartitionOffset) bool {
	topic, partition, group, offset := newOffset.Topic,
		newOffset.Partition, newOffset.Group, newOffset.Offset
	tmp, _ := qm.OffsetStore.LoadOrStore(topic, new(syncmap.Map))
	tpOffsetMap, _ := tmp.(*syncmap.Map)

	tmp, _ = tpOffsetMap.LoadOrStore(partition, new(syncmap.Map))
	pOffsetMap, _ := tmp.(*syncmap.Map)

	pOffsetMap.Store(group, offset)
	return true
}

// Sends the gauge to Statsd.
func (qm *QueueMonitor) sendGaugeToStatsd(stat string, value int64) {
	if qm.StatsdClient == nil {
		log.Println("Statsd Client not initialized yet.")
		return
	}
	err := qm.StatsdClient.Gauge(stat, value)
	if err != nil {
		log.Println("Error while sending gauge to statsd:", err)
		return
	}
	log.Printf("Gauge sent to Statsd: %s=%d", stat, value)
}
