package main

import (
	"log"
)

func main() {
	qsm, err := NewQueueSizeMonitor([]string{"localhost:9092"})
	if err != nil {
		log.Fatalln(err)
	}
	_, err = qsm.GetConsumerPartitionOffsets()
	if err != nil {
		log.Fatalln(err)
	}
}
