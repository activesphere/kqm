package main

import (
	"github.com/Shopify/sarama"
)

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

// OffsetMap : Aggregated type for Group -> Topic -> Partition -> Offset.
type OffsetMap map[string]map[string]map[int32]int64
