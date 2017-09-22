package main

import (
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/quipo/statsd"
	"golang.org/x/sync/syncmap"
)

// QueueMonitor : Defines the type for Kafka Queue Monitor implementation.
type QueueMonitor struct {
	Client       sarama.Client
	StatsdClient *statsd.StatsdClient
	Config       *QMConfig
	OffsetStore  *syncmap.Map
}

// PartitionOffset : Defines a type for Partition Offset
type PartitionOffset struct {
	Topic         string
	Partition     int32
	Offset        int64
	Timestamp     int64
	Group         string
	DueForRemoval bool
}

// BrokerOffsetRequest : Aggregated type for Broker and OffsetRequest
type BrokerOffsetRequest struct {
	Broker        *sarama.Broker
	OffsetRequest *sarama.OffsetRequest
}

// PartitionConsumers : Wrapper around a list of sarama.PartitionConsumer
type PartitionConsumers struct {
	Handles   []sarama.PartitionConsumer
	mutex     *sync.Mutex
	areClosed bool
}

// Add : Appends a partition consumer to the consumers list.
func (pc *PartitionConsumers) Add(pConsumer sarama.PartitionConsumer) {
	pc.Handles = append(pc.Handles, pConsumer)
}

// AsyncCloseAll : Calls AsyncClose() on all Partition Consumers.
func (pc *PartitionConsumers) AsyncCloseAll() {
	defer pc.mutex.Unlock()
	pc.mutex.Lock()
	if pc.areClosed {
		return
	}
	for _, pConsumer := range pc.Handles {
		pConsumer.AsyncClose()
	}
	pc.areClosed = true
	log.Println("AsyncClose() called on all Partition Consumers.")
}

// KafkaConfig : Type for Kafka Broker Configuration.
type KafkaConfig struct {
	Brokers []string
}

// StatsdConfig : Type for Statsd Client Configuration.
type StatsdConfig struct {
	Addr   string
	Prefix string
}

// QMConfig : Aggregated type for all configuration required for KQM.
type QMConfig struct {
	KafkaCfg     KafkaConfig
	StatsdCfg    StatsdConfig
	ReadInterval time.Duration
}
