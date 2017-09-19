package main

import (
	"time"
	"github.com/Shopify/sarama"
)

// Monitor : Interface for monitor types.
type Monitor interface {
	Start(readInterval time.Duration, retryInterval time.Duration)
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
	handle    sarama.PartitionConsumer
	isClosed  bool
}

// AsyncClose : Wrapper around sarama.PartitionConsumer.AsyncClose()
func (pc *PartitionConsumer) AsyncClose() {
	if(pc.isClosed) {
		return
	}
	pc.isClosed = true
	pc.handle.AsyncClose()
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

// QSMConfig : Aggregated type for all configuration required for KQSM.
type QSMConfig struct {
	KafkaCfg           KafkaConfig
	StatsdCfg          StatsdConfig
	ReadInterval       time.Duration
	RetryInterval      time.Duration
	MaxRetries         int
}
