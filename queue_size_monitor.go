package main

import (
	"fmt"
	"github.com/Shopify/sarama"
)

// QueueSizeMonitor : Defines the type for Kafka Queue Size 
// Monitor implementation using Sarama.
type QueueSizeMonitor struct {
	Client sarama.Client
	Groups []ConsumerGroup
}

// NewQueueSizeMonitor : Returns a QueueSizeMonitor with an initialized client
// based on the comma-separated brokers (eg. "localhost:9092")
func (qsm *QueueSizeMonitor) NewQueueSizeMonitor(brokers []string) (*QueueSizeMonitor, error) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(brokers, config)
	qsm = &QueueSizeMonitor{}
	qsm.Client = client
	return qsm, err
}

// ConnectBroker : Connects to a broker
func (qsm *QueueSizeMonitor) ConnectBroker(broker *sarama.Broker) error {
	if ok, _ := broker.Connected(); ok {
		return nil
	}

	err := broker.Open(qsm.Client.Config())
	if err != nil {
		return err
	}

	connected, err := broker.Connected()
	if err != nil {
		return err
	}

	if !connected {
		return fmt.Errorf("Failed to connect to broker: %#v", broker.Addr())
	}
}

// GetConsumerGroups : Returns a list of Consumer Groups associated with the
// client.
func (qsm *QueueSizeMonitor) GetConsumerGroups() ([]ConsumerGroup, error) {
	brokers := qsm.Client.Brokers()
	consumerGroups := make([]ConsumerGroup, 10)
	for _, broker := range brokers {
		
	}
}
