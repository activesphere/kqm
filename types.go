package main

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/quipo/statsd"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/syncmap"
)

// QueueMonitor : Defines the type for Kafka Queue Monitor implementation.
type QueueMonitor struct {
	Client             sarama.Client
	StatsdClient       *statsd.StatsdClient
	Config             *QMConfig
	OffsetStore        *syncmap.Map
	PartitionConsumers *PartitionConsumers
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
	Handles []sarama.PartitionConsumer
	mutex   *sync.Mutex
}

// Add : Appends a partition consumer to the partition consumers list.
func (pc *PartitionConsumers) Add(pConsumer sarama.PartitionConsumer) {
	defer pc.mutex.Unlock()
	pc.mutex.Lock()
	pc.Handles = append(pc.Handles, pConsumer)
}

// PurgeHandles : Purges all Partition Consumers (Handles) by calling
// AsyncClose() on each of them. After doing so, it truncates the list of
// Partition Consumers.
func (pc *PartitionConsumers) PurgeHandles() {
	defer pc.mutex.Unlock()
	pc.mutex.Lock()
	for _, pConsumer := range pc.Handles {
		pConsumer.AsyncClose()
	}
	pc.Handles = make([]sarama.PartitionConsumer, 0)
	log.Infoln("Purged Partition Consumers.")
}

// NewPartitionConsumers : Creates a new PartitionsConsumers instance.
func NewPartitionConsumers() *PartitionConsumers {
	return &PartitionConsumers{
		Handles: make([]sarama.PartitionConsumer, 0),
		mutex:   &sync.Mutex{},
	}
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
