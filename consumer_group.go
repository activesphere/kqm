package main

// ConsumerGroup : Defines the type for a Kafka Consumer Group
type ConsumerGroup struct {
	Name string
	Topics []Topic
}

// ConsumerGroupManager : Defines actions for a consumer group management type
type ConsumerGroupManager interface {
	GetConsumerGroups() ([]ConsumerGroup, error)
}
