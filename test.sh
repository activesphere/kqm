#!/bin/bash

# Update dependencies
apt-get clean && apt-get update
apt-get install -y python zookeeper zookeeperd wget software-properties-common git net-tools

# Install Go
add-apt-repository -y ppa:longsleep/golang-backports
apt-get update
apt-get install -y golang-go

# Install Kafka
wget http://redrockdigimark.com/apachemirror/kafka/0.11.0.2/kafka_2.11-0.11.0.2.tgz
tar xvf kafka_2.11-0.11.0.2.tgz
mv kafka_2.11-0.11.0.2 kafka

# Install librdkafka v0.11.1
wget https://github.com/edenhill/librdkafka/archive/v0.11.1.tar.gz
tar xvf v0.11.1.tar.gz
pushd /kqm/librdkafka-0.11.1
./configure --prefix /usr && make && make install
popd

# Setup KQM and KQM Test Runner
export GOPATH=/kqm/go
mkdir -p /kqm/go/src/github.com/activesphere
pushd /kqm/go/src/github.com/activesphere
git clone https://github.com/activesphere/kqm
popd

function findproc() {
	echo $(ps aux | grep -i $1 | grep -v grep)
}

echo "Starting Zookeeper."
service zookeeper start
echo "Waiting for 15 seconds."
sleep 15
echo "Zookeeper: $(findproc zookeeper)"

echo "Starting Kafka."
nohup kafka/bin/kafka-server-start.sh kafka/config/server.properties >kafka.log \
	2>&1 &
echo "Waiting for 15 seconds."
sleep 15
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
echo "Waiting for half a minute."
sleep 30
echo "KQM: $(findproc kqm)"

echo "Start a Consumer to the __consumer_offsets topic."
nohup /kqm/kafka/bin/kafka-console-consumer.sh --topic __consumer_offsets \
    --bootstrap-server localhost:9092 \
    --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" \
    --from-beginning > /kqm/consumer.log 2>&1 &
echo "Waiting for 15 seconds."
sleep 15
echo "Consumer: $(findproc ConsoleConsumer)"

echo "KQM Port Status: $(netstat -anlp | grep -i 8125)"

echo "Running tests."
pushd tests
go test -v lag_test.go
