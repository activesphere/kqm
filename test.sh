#!/bin/bash
function findproc() {
	echo $(ps aux | grep -i $1 | grep -v grep)
}

pushd /kqm

echo "Starting Zookeeper."
service zookeeper start
sleep 10
echo "Zookeeper: $(findproc zookeeper)"

echo "Starting Kafka."
nohup kafka/bin/kafka-server-start.sh kafka/config/server.properties >kafka.log \
	2>&1 &
sleep 10
echo "Kafka: $(findproc kafka)"

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

echo "Starting KQM."
nohup ./kqm --log-level=5 \
	--interval=1 \
	--statsd-addr localhost:8125 \
	--statsd-prefix prefix_demo \
	localhost:9092 > kqm.log 2>&1 &
sleep 10
echo "KQM: $(findproc kqm)"

echo "Start a Consumer to the __consumer_offsets topic."
nohup /kqm/kafka/bin/kafka-console-consumer.sh --topic __consumer_offsets \
    --bootstrap-server localhost:9092 \
    --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" \
    --from-beginning > consumer.log 2>&1 &
sleep 10
echo "Consumer: $(findproc ConsoleConsumer)"

echo "KQM Port Status: $(netstat -anlp | grep -i 8125)"

echo "Running tests."
pushd tests
go test -v lag_test.go
