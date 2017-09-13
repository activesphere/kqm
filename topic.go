package main

// Topic : Defines a type for Kafka Topic
type Topic struct {
	Name string
	Partition *Partition
}
