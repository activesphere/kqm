package main

import (
	"time"
)

func main() {
	cfg := &QSMConfig{
		KafkaCfg: KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
		StatsdCfg: StatsdConfig{
			Addr: "172.19.0.3:8125",
			Prefix: "kqsm_prefix",
		},
		ReadInterval: 10 * time.Second,
		RetryInterval: 5 * time.Second,
	}
	Start(cfg)
}
