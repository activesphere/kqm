package main

import (
	"time"
	"fmt"
	"sync"
	"bytes"
	"encoding/binary"
	"log"
	"golang.org/x/sync/syncmap"
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
	ConsumerOffsetStore       *syncmap.Map
	wgBrokerOffsetResponse    sync.WaitGroup
	BrokerOffsetStore         *syncmap.Map
	StatsdClient              *statsd.StatsdClient
	Config                    *QSMConfig
}

// RetryOnFailure : It retries the func passed an argument
// based on the whether the the fn returns an error or the
// Error channel receives an error or not.
func RetryOnFailure(cfg *QSMConfig, title string, fn func(ec chan error) error) {
	errorChannel := make(chan error)
	var err error
	for {
		err = fn(errorChannel)
		if err != nil {
			log.Println("Retrying due to a sychronous error:", title)
			time.Sleep(cfg.RetryInterval)
			continue
		}
		err = <- errorChannel
		if err != nil {
			log.Println("Retrying due to an asychronous error:", title)
			time.Sleep(cfg.RetryInterval)
			continue
		}
		log.Println("Completed Execution Successfully:", title)
		break
	}
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
		RetryOnFailure(cfg, "CONSUMER_OFFSETS", func(ec chan error) error {
			return qsm.GetConsumerOffsets(ec)
		})
	}()

	for {
		RetryOnFailure(cfg, "REPORT_LAG", func(ec chan error) error {
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
	// Setting Consumer.Return.Errors to true enables sending the Partition
	// Consumer Errors to the Error Channel instead of logging them.
	config.Consumer.Return.Errors = true
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
	qsm.ConsumerOffsetStore = new(syncmap.Map)
	qsm.BrokerOffsetStore = new(syncmap.Map)
	qsm.StatsdClient = statsdClient
	qsm.Config = cfg
	return qsm, err
}

// GetConsumerOffsets : Subcribes to Offset Topic and parses messages to
// obtains Consumer Offsets.
func (qsm *QueueSizeMonitor) GetConsumerOffsets(errorChannel chan error) error {
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

	partitionConsumers := make([]PartitionConsumer, len(partitions))

	closePartitionConsumers := func() {
		for _, pConsumer := range partitionConsumers {
			pConsumer.AsyncClose()
		}
	}

	consumeMessage := func(pConsumer PartitionConsumer) {
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

		for message := range pConsumer.Handle.Messages() {
			formatAndStoreMessage(message)
		}
		closePartitionConsumers()
	}

	checkErrors := func(pConsumer PartitionConsumer) {
		errorChannel <- (<- pConsumer.Handle.Errors()).Err
		closePartitionConsumers()
	}

	for i, partition := range partitions {
		pConsumer, err := consumer.ConsumePartition(ConsumerOffsetTopic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Println("Error occured while consuming partition.", err)
			closePartitionConsumers()
			return err
		}
		partitionConsumers[i] = PartitionConsumer{Handle: pConsumer, isClosed: false}
	}

	for _, pConsumer := range partitionConsumers {
		go consumeMessage(pConsumer)
		go checkErrors(pConsumer)
	}

	return nil
}

// GetBrokerOffsets : Finds out the leader brokers for the partitions and
// gets the latest commited offsets.
func (qsm *QueueSizeMonitor) GetBrokerOffsets() error {

	tpMap := qsm.getTopicsAndPartitions(qsm.ConsumerOffsetStore)
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
func (qsm *QueueSizeMonitor) getTopicsAndPartitions(offsetStore *syncmap.Map) map[string][]int32 {
	tpMap := make(map[string][]int32)
	offsetStore.Range(func(_, gbodyI interface{}) bool {
		gbodyI.(*syncmap.Map).Range(func(topicI, tbodyI interface{}) bool {
			topic := topicI.(string)
			tbodyI.(*syncmap.Map).Range(func(partitionI, _ interface{}) bool {
				tpMap[topic] = append(tpMap[topic], partitionI.(int32))
				return true
			})
			return true
		})
		return true
	})
	return tpMap
}

// Computes the lag and sends the data as a gauge to Statsd.
func (qsm *QueueSizeMonitor) computeLag(brokerOffsetMap *syncmap.Map, consumerOffsetMap *syncmap.Map) error {
	consumerOffsetMap.Range(func(groupI, gbodyI interface{}) bool {
		group := groupI.(string)
		gbodyI.(*syncmap.Map).Range(func(topicI, tbodyI interface{}) bool {
			topic := topicI.(string)
			tbodyI.(*syncmap.Map).Range(func(partitionI, offsetI interface{}) bool {
				partition := partitionI.(int32)
				consumerOffset := offsetI.(int64)

				tmp, _ := brokerOffsetMap.Load(topic)
				brokerPMap := tmp.(*syncmap.Map)
				tmp, _ = brokerPMap.Load(partition)
				brokerOffset := tmp.(int64)

				lag := brokerOffset - consumerOffset

				stat := fmt.Sprintf("%s.group.%s.%s.%d",
					qsm.Config.StatsdCfg.Prefix, group, topic, partition)
				if lag < 0 {
					log.Printf("Negative Lag received for %s: %d", stat, lag)
					return true
				}
				go qsm.sendGaugeToStatsd(stat, lag)
				log.Printf("\n+++++++++(Topic: %s, Partn: %d)++++++++++++" +
					"\nBroker Offset: %d" +
					"\nConsumer Offset: %d" +
					"\nLag: %d" +
					"\n++++++++++(Group: %s)+++++++++++",
					topic, partition, brokerOffset, consumerOffset, lag, group)

				return true
			})
			return true
		})
		return true
	})
	return nil
}

// Store newly received consumer offset.
func (qsm *QueueSizeMonitor) storeConsumerOffset(newOffset *PartitionOffset) {
	group, topic, partition, offset := newOffset.Group, newOffset.Topic,
		newOffset.Partition, newOffset.Offset
	tmp, _ := qsm.ConsumerOffsetStore.LoadOrStore(group, new(syncmap.Map))
	tpOffsetMap := tmp.(*syncmap.Map)
	tmp, _ = tpOffsetMap.LoadOrStore(topic, new(syncmap.Map))
	pOffsetMap := tmp.(*syncmap.Map)
	pOffsetMap.Store(partition, offset)
}

// Store newly received broker offset.
func (qsm *QueueSizeMonitor) storeBrokerOffset(newOffset *PartitionOffset) {
	topic, partition, offset := newOffset.Topic, newOffset.Partition, newOffset.Offset
	tmp, _ := qsm.BrokerOffsetStore.LoadOrStore(topic, new(syncmap.Map))
	pOffsetMap := tmp.(*syncmap.Map)
	pOffsetMap.Store(partition, offset)
}

// Sends the gauge to Statsd.
func (qsm *QueueSizeMonitor) sendGaugeToStatsd(stat string, value int64) {
	if qsm.StatsdClient == nil {
		log.Println("Statsd Client not initialized yet.")
		return
	}
	err := qsm.StatsdClient.Gauge(stat, value)
	if err != nil {
		log.Println("Error while sending gauge to statsd:", err)
		return
	}
	log.Printf("Gauge sent to Statsd: %s=%d", stat, value)
}

