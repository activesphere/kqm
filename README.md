**KQM**: Lag Monitor for Kafka [![Build Status](https://travis-ci.org/activesphere/kqm.svg?branch=master)](https://travis-ci.org/activesphere/kqm)
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
```

Example
-------------------
```
kqm --log-level=5 \
    --interval=30 \
    --statsd-addr localhost:8125 \
    --statsd-prefix prefix_demo \
    localhost:9092
```
