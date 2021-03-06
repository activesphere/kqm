package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/activesphere/kqm/monitor"
	log "github.com/sirupsen/logrus"
)

var description = `
kqm [OPTIONS] host:port [host:port]...

KQM is a command line tool to monitor Apache Kafka for lags.
It also comes with an option to send the lag statistics to Statsd.

Option               Description
------               -----------
--statsd-addr        Use this option if you need to send
                     the lag statistics to Statsd.
                     Default: localhost:8125

--statsd-prefix      Set a prefix for the data being sent
                     to Statsd.
                     Default: kqm

--interval           Specify the interval of calculating
                     the lag statistics (in seconds).
                     Default: 60 seconds

--log-level          Specify the level of severity of the
                     logger. Levels are as follows:
                     0 - Panic
                     1 - Fatal
                     2 - Error (Default)
                     3 - Warn
                     4 - Info
                     5 - Debug

Example Command Usage:
kqm --log-level=5 \
    --interval=30 \
    --statsd-addr localhost:8125 \
    --statsd-prefix prefix_demo \
    localhost:9092
`

func parseCommand() (*monitor.QMConfig, error) {

	var (
		brokers                  []string
		interval, logLevel       *int
		statsdAddr, statsdPrefix *string
	)

	interval = flag.Int("interval", 60, "")
	statsdAddr = flag.String("statsd-addr", "localhost:8125", "")
	statsdPrefix = flag.String("statsd-prefix", "kqm", "")
	logLevel = flag.Int("log-level", 2, "")
	flag.Usage = func() {
		fmt.Println(description)
	}
	flag.Parse()

	brokers = flag.Args()
	if len(brokers) == 0 {
		return nil, fmt.Errorf("Please specify brokers")
	}

	cfg := &monitor.QMConfig{
		KafkaCfg: monitor.KafkaConfig{
			Brokers: brokers,
		},
		StatsdCfg: monitor.StatsdConfig{
			Addr:   *statsdAddr,
			Prefix: *statsdPrefix,
		},
		Interval: time.Duration(*interval) * time.Second,
	}

	log.SetLevel(log.AllLevels[*logLevel])
	return cfg, nil
}

func main() {
	cfg, err := parseCommand()
	if err != nil {
		fmt.Printf("%s\n%s", err, description)
		os.Exit(1)
	}
	monitor.Start(cfg)
}
