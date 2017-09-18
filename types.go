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

// POffsetMap : Custom type for Partition -> Offset.
type POffsetMap map[int32]int64

// TPOffsetMap : Aggregated type for Topic -> Partition -> Offset.
type TPOffsetMap map[string]POffsetMap

// GTPOffsetMap : Aggregated type for Group -> Topic -> Partition -> Offset.
type GTPOffsetMap map[string]TPOffsetMap

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
