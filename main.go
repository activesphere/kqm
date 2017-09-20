package main

import (
	"fmt"
	"os"
	"strings"
	"time"
	"strconv"
)

var description = `
kqm --brokers host:port,[host:port...] [OPTIONS]

KQM is a command line tool to monitor Apache Kafka for lags.
It also comes with an option to send the lag statistics to Statsd.

Option										Description
------										-----------
--statsd-addr								Use this option if you need to send
											the lag statistics to Statsd.
--statsd-prefix								This option is REQUIRED IF
											--statsd-addr is specified.
--read-interval								Specify the interval of calculating
											the lag statistics (in seconds).
											DEFAULT: 120 seconds
`

var flagProps = map[string]string{
	"--brokers":		"brokers",
	"--statsd-addr":	"statsdAddr",
	"--statsd-prefix":	"statsdPrefix",
	"--read-interval":	"readInterval",
}

func parseCLIArgs(args []string) (*QMConfig, error) {

	getInt := func(propVals map[string]string, key string) (int, bool, error) {
		propVal, ok := propVals[key]
		if !ok {
			return -1, false, nil
		}
		result, err := strconv.Atoi(propVal)
		if err != nil {
			return -1, false, fmt.Errorf("Integer Conversion Failed")
		}
		return result, true, nil
	}

	argsLen := len(args)
	propVals := make(map[string]string)

	if (argsLen - 1) % 2 != 0 {
		return nil, fmt.Errorf("Please specify value for each flag")
	}

	for i := 1; i < argsLen - 1; {
		flag, ok := flagProps[args[i]]
		if !ok {
			return nil, fmt.Errorf("Invalid flag: %s", args[i])
		}
		val := args[i + 1]
		propVals[flag] = val
		i += 2
	}

	var (
		brokers []string
		readInterval int
		statsdAddr, statsdPrefix string
	)

	brokerStr, ok := propVals["brokers"]
	if !ok {
		return nil, fmt.Errorf(`Please specify brokers. Example:
			kqm --brokers localhost:9092,localhost:9093`)
	}
	brokers = strings.Split(brokerStr, ",")

	cfg := &QMConfig{
		KafkaCfg: KafkaConfig{
			Brokers: brokers,
		},
		ReadInterval: 10 * time.Second,
	}

	readInterval, ok, err := getInt(propVals, "readInterval")
	if(!ok) {
		return cfg, nil
	}
	if err != nil {
		return nil, fmt.Errorf("Invalid value for flag: --read-interval")
	}
	cfg.ReadInterval = time.Duration(readInterval) * time.Second

	statsdAddr, hasAddr := propVals["statsdAddr"]
	statsdPrefix, hasPrefix := propVals["statsdPrefix"]
	if (hasAddr && !hasPrefix) || (!hasAddr && hasPrefix) {
		return cfg, fmt.Errorf("Addr and Prefix both required for Statsd")
	}
	cfg.StatsdCfg = StatsdConfig{ Addr: statsdAddr, Prefix: statsdPrefix }

	return cfg, nil
}

func main() {
	cfg, err := parseCLIArgs(os.Args)
	if err != nil {
		fmt.Printf("%s\n%s", err, description)
		os.Exit(1)
	}
	Start(cfg)
}
