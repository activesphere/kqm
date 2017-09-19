**KQM**: Lag Monitor for Kafka
===================

What it is
-------------

KQM is an interval-based lag monitor for Apache Kafka (>=0.9) written in Go. It calculates the lag and sends it to [Statsd](https://github.com/etsy/statsd "Statsd") after every `interval`, where `interval` is provided by the user in the configuration.

Installation
-------------------
```
go get -u github.com/activesphere/kqm
```


Usage
-------------------
```
import (
	"time"
	"github.com/activesphere/kqm"
)

cfg := &QSMConfig{
	KafkaCfg: KafkaConfig{
		Brokers: []string{"localhost:9092"},
	},
	StatsdCfg: StatsdConfig{
		Addr: "172.19.0.3:8125",
		Prefix: "kqsm_prefix",
	},
	ReadInterval: 2 * time.Minute,
	RetryInterval: 5 * time.Second,
}
Start(cfg)
```

**Use ^C to terminate the execution.**
