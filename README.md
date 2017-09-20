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
kqm --brokers host:port,[host:port...] [OPTIONS]

KQM is a command line tool to monitor Apache Kafka for lags.
It also comes with an option to send the lag statistics to Statsd.

Option              Description
------              -----------
--statsd-addr       Use this option if you need to send
                    the lag statistics to Statsd.

--statsd-prefix     This option is REQUIRED IF
                    --statsd-addr is specified.

--read-interval     Specify the interval of calculating
                    the lag statistics (in seconds).
                    DEFAULT: 120 seconds
```

Example
-------------------
```
kqm --brokers localhost:9092 --read-interval 1 \
	--statsd-addr localhost:8125 --statsd-prefix prefix_demo
```
