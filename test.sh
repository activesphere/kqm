#!/bin/bash

function find_proc() {
	echo $(ps aux | grep -i $1 | grep -v grep)
}

function log_kqm_status() {
	echo "KQM Port Status: $(netstat -anlp | grep -i 8125)"
	count=1
	while [ $count -le 3 ]
	do
	echo "KQM Output:"
	tail -n 5 /kqm/kqm.log
	echo "Waiting for 10 seconds."
	sleep 10
	((count++))
	done
}

function start_consumer() {
	pushd /kqm
	echo "Start a Consumer to the $1 topic."
	nohup kafka/bin/kafka-console-consumer.sh --topic $1 \
		--bootstrap-server localhost:9092 \
		--formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" \
		--from-beginning > $1.log 2>&1 &
	echo "Waiting for 15 seconds."
	sleep 15
	echo "Consumer: $(find_proc $1)"
	echo "Consumer Output:"
	tail -n 5 "$1.log"
	popd
}

echo "Installing dependencies."
./install.sh $1 > /dev/null 2>&1
echo "Dependency installation complete."

echo "Starting Zookeeper."
service zookeeper start
echo "Waiting for 15 seconds."
sleep 15
echo "Zookeeper: $(find_proc zookeeper)"
echo "Zookeeper Log File:"
tail -n 5 /var/log/zookeeper/zookeeper.log

echo "Starting Kafka."
nohup kafka/bin/kafka-server-start.sh kafka/config/server.properties >kafka.log \
	2>&1 &
echo "Waiting for 15 seconds."
sleep 15
echo "Kafka: $(find_proc kafka)"
echo "Kafka Log File:"
tail -n 5 kafka.log

echo "Creating a Kafka Topics."
kafka/bin/kafka-topics.sh --create --topic topic1 --zookeeper localhost:2181 \
	--partitions 4 --replication-factor 1
kafka/bin/kafka-topics.sh --create --topic topic2 --zookeeper localhost:2181 \
	--partitions 4 --replication-factor 1
kafka/bin/kafka-topics.sh --create --topic topic3 --zookeeper localhost:2181 \
	--partitions 4 --replication-factor 1
kafka/bin/kafka-topics.sh --create --topic topic4 --zookeeper localhost:2181 \
	--partitions 4 --replication-factor 1

export GOPATH=/kqm/go
pushd /kqm/go/src/github.com/activesphere/kqm

echo "Building KQM."
go build

start_consumer __consumer_offsets

echo "Starting KQM."
nohup ./kqm --log-level=2 \
	--interval=1 \
	--statsd-addr localhost:8125 \
	--statsd-prefix prefix_demo \
	localhost:9092 > /kqm/kqm.log 2>&1 &
echo "Waiting for 15 seconds."
sleep 15
echo "KQM: $(find_proc kqm)"

log_kqm_status

echo "Running tests."
pushd tests
go test -timeout 20m -v lag_test.go
