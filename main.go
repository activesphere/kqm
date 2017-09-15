package main

import (
	"time"
	"log"
)

func main() {
	statsdCfg := StatsdConfig{addr: "localhost:8125", prefix: "kqsm_prefix",}
	qsm, err := NewQueueSizeMonitor([]string{"localhost:9092"}, statsdCfg)
	if err != nil {
		log.Println(err)
	}
	qsm.Start(2 * time.Minute)
}
