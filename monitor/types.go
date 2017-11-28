package monitor

import (
	"fmt"
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

func (p *PartitionOffset) String() string {
	return fmt.Sprintf("Topic: %s, Partn: %d, Offset: %d, Group: %s, "+
		"DueForRemoval: %t", p.Topic, p.Partition, p.Offset, p.Group,
		p.DueForRemoval)
}

// BrokerOffsetRequest : Aggregated type for Broker and OffsetRequest
type BrokerOffsetRequest struct {
	Broker        *sarama.Broker
	OffsetRequest *sarama.OffsetRequest
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
	KafkaCfg  KafkaConfig
	StatsdCfg StatsdConfig
	Interval  time.Duration
}
