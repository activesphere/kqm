package main

// Partition : Defines a type for Kafka Partition
type Partition struct {
	Name string
	Lag *int
}
