package main

import (
	"log"
)

func main() {
	statsdCfg := StatsdConfig{addr: "localhost:8125", prefix: "kqsm_prefix",}
	qsm, err := NewQueueSizeMonitor([]string{"localhost:9092"}, statsdCfg)
	if err != nil {
		log.Fatalln(err)
	}
	qsm.Start()
}
