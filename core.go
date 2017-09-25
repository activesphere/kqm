package main

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/quipo/statsd"
	log "github.com/sirupsen/logrus"
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
			log.Errorln("Retrying due to a sychronous error:", title)
			time.Sleep(cfg.ReadInterval)
			continue
		}
		log.Infoln("Completed Execution Successfully:", title)
		break
	}
}

// RetryWithChannel : It retries the func passed an argument
// based on the whether the the fn returns an error or the
// Error channel receives an error or not.
func RetryWithChannel(cfg *QMConfig, title string, fn func(ec chan error) error) {
	errorChannel := make(chan error)
	resetErrorChannel := func() {
		close(errorChannel)
		errorChannel = make(chan error)
	}
	var err error
	for {
		err = fn(errorChannel)
		if err != nil {
			log.Errorln("Retrying due to a error returned by fn:", title)
			time.Sleep(cfg.ReadInterval)
			continue
		}
		err = <-errorChannel
		if err != nil {
			log.Errorln("Retrying due to a error received from channel:", title)
			resetErrorChannel()
			time.Sleep(cfg.ReadInterval)
			continue
		}
		log.Infoln("Completed Execution Successfully:", title)
		break
	}
}

// Start : Initiates the monitoring procedure, prints out the lag results
// and sends the results to Statsd.
func Start(cfg *QMConfig) {
	qm, err := NewQueueMonitor(cfg)
	if err != nil {
		log.Errorln("Error while creating QueueMonitor instance.", err)
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
	client, err := sarama.NewClient(cfg.KafkaCfg.Brokers, config)
	if err != nil {
		return nil, err
	}
	statsdClient := statsd.NewStatsdClient(cfg.StatsdCfg.Addr,
		cfg.StatsdCfg.Prefix)
	err = statsdClient.CreateSocket()
	if err != nil {
		return nil, err
	}
	qm := &QueueMonitor{}
	qm.Client = client
	qm.OffsetStore = new(syncmap.Map)
	qm.Config = cfg
	qm.StatsdClient = statsdClient
	return qm, err
}

// GetConsumerOffsets : Subcribes to Offset Topic and parses messages to
// obtains Consumer Offsets.
func (qm *QueueMonitor) GetConsumerOffsets(errorChannel chan error) error {
	log.Infoln("Started getting consumer partition offsets.")

	consumeMessage := func(pcIndex int) {
		defer func() {
			defer func() {
				if rc := recover(); rc != nil {
					log.Warningln("Recovering write to closed channel.")
				}
			}()
			errorChannel <- fmt.Errorf("Message Channel Closed")
		}()
		messageChannel := qm.PartitionConsumers.Handles[pcIndex].Messages()
		for message := range messageChannel {
			partitionOffset, err := parseConsumerMessage(message)
			if err != nil {
				log.Errorln("Error while parsing consumer message:", err)
				continue
			}
			if partitionOffset != nil {
				if partitionOffset.DueForRemoval {
					qm.removeConsumerGroup(partitionOffset)
				} else {
					qm.storeConsumerOffset(partitionOffset)
				}
			}
		}
	}

	partitions, err := qm.Client.Partitions(ConsumerOffsetTopic)
	if err != nil {
		log.Errorln("Error occured while getting client partitions.", err)
		return err
	}
	consumer, err := sarama.NewConsumerFromClient(qm.Client)
	if err != nil {
		log.Errorln("Error occured while creating new client consumer.", err)
		return err
	}

	if qm.PartitionConsumers != nil {
		qm.PartitionConsumers.PurgeHandles()
	} else {
		qm.PartitionConsumers = NewPartitionConsumers()
	}

	for _, partition := range partitions {
		pConsumer, err := consumer.ConsumePartition(ConsumerOffsetTopic,
			partition, sarama.OffsetNewest)
		if err != nil {
			log.Errorln("Error occured while creating Consumer Partition.", err)
			return err
		}
		qm.PartitionConsumers.Add(pConsumer)
	}

	for index := range qm.PartitionConsumers.Handles {
		go consumeMessage(index)
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
				log.Errorln("Error occured while fetching leader broker:", err)
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
			log.Errorln("Error while getting available offsets from broker.", err)
			return err
		}

		for topic, partitionMap := range response.Blocks {
			for partition, offsetResponseBlock := range partitionMap {
				if offsetResponseBlock.Err != sarama.ErrNoError {
					log.Errorln("Error in offset response block.",
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
	tpOffsetMap, ok := tmp.(*syncmap.Map)
	if !ok {
		return fmt.Errorf("Not a valid syncmap at Topic: %s", topic)
	}
	tmp, ok = tpOffsetMap.Load(partition)
	if !ok {
		return fmt.Errorf("Partition doesn't exist in syncmap: %d", partition)
	}
	pOffsetMap, ok := tmp.(*syncmap.Map)
	if !ok {
		return fmt.Errorf("Not a valid syncmap at Partition: %d", partition)
	}
	pOffsetMap.Range(func(groupI, offsetI interface{}) bool {
		group, ok := groupI.(string)
		if !ok {
			log.Warningln("Invalid cast to string for group.")
			return false
		}
		offset, ok := offsetI.(int64)
		if !ok {
			log.Warningln("Invalid cast to int64 for offset.")
			return false
		}
		lag := brokerOffset - offset
		if lag < 0 {
			lag = 0
		}
		stat := fmt.Sprintf(".group.%s.%s.%d", group, topic, partition)
		go qm.sendGaugeToStatsd(stat, lag)
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

// Remove a Consumer Group from the Offset Store.
func (qm *QueueMonitor) removeConsumerGroup(p *PartitionOffset) bool {
	topic, partition, group := p.Topic, p.Partition, p.Group

	tmp, ok := qm.OffsetStore.Load(topic)
	if !ok {
		return false
	}
	tpOffsetMap, _ := tmp.(*syncmap.Map)
	tmp, ok = tpOffsetMap.Load(partition)
	if !ok {
		return false
	}
	pOffsetMap, _ := tmp.(*syncmap.Map)
	pOffsetMap.Delete(group)

	log.Infoln("Removed topic: %s partition: %d group: %s",
		topic, partition, group)
	return true
}

// Sends the gauge to Statsd.
func (qm *QueueMonitor) sendGaugeToStatsd(stat string, value int64) {
	if qm.StatsdClient == nil {
		log.Warningln("Statsd Client not initialized yet.")
		return
	}
	err := qm.StatsdClient.Gauge(stat, value)
	if err != nil {
		log.Errorln("Error while sending gauge to statsd:", err)
		return
	}
	log.Infoln("Gauge sent to Statsd: %s=%d", stat, value)
}
