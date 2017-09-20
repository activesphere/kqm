package main

import (
	"sync"
	"log"
	"time"
	"github.com/Shopify/sarama"
	"github.com/quipo/statsd"
	"golang.org/x/sync/syncmap"
)

// QueueMonitor : Defines the type for Kafka Queue Monitor implementation.
type QueueMonitor struct {
	Client                    sarama.Client
	wgConsumerMessages        sync.WaitGroup
	ConsumerOffsetStore       *syncmap.Map
	wgBrokerOffsetResponse    sync.WaitGroup
	BrokerOffsetStore         *syncmap.Map
	StatsdClient              *statsd.StatsdClient
	Config                    *QMConfig
}

// PartitionOffset : Defines a type for Partition Offset
type PartitionOffset struct {
	Topic               string
	Partition           int32
	Offset              int64
	Timestamp           int64
	Group               string
}

// BrokerOffsetRequest : Aggregated type for Broker and OffsetRequest
type BrokerOffsetRequest struct {
	Broker          *sarama.Broker
	OffsetRequest   *sarama.OffsetRequest
}

// PartitionConsumer : Wrapper around sarama.PartitionConsumer
type PartitionConsumer struct {
	Handle        sarama.PartitionConsumer
	HandleMutex   *sync.Mutex
	isClosed      bool
}

// AsyncClose : Wrapper around sarama.PartitionConsumer.AsyncClose()
func (pc *PartitionConsumer) AsyncClose() {
	defer pc.HandleMutex.Unlock()
	pc.HandleMutex.Lock()
	if pc.Handle == nil {
		log.Println("Partition Consumer is uninitialized.")
		return
	}
	if pc.isClosed {
		log.Println("Partition Consumer is already closed.")
		return
	}
	log.Println("Closing Partition Consumer...")
	pc.Handle.AsyncClose()
	pc.isClosed = true
}

// KafkaConfig : Type for Kafka Broker Configuration.
type KafkaConfig struct {
	Brokers []string
}

// StatsdConfig : Type for Statsd Client Configuration.
type StatsdConfig struct {
	Addr    string
	Prefix  string
}

// QMConfig : Aggregated type for all configuration required for KQM.
type QMConfig struct {
	KafkaCfg           KafkaConfig
	StatsdCfg          StatsdConfig
	ReadInterval       time.Duration
	RetryInterval      time.Duration
}
