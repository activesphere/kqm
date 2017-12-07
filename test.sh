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
echo "Zookeeper Log File:"
cat /var/log/zookeeper/zookeeper.log

echo "Starting Kafka."
nohup kafka/bin/kafka-server-start.sh kafka/config/server.properties >kafka.log \
	2>&1 &
echo "Waiting for 15 seconds."
sleep 15
echo "Kafka: $(findproc kafka)"
echo "Kafka Log File:"
cat kafka.log

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
	localhost:9092 > /kqm/kqm.log 2>&1 &
echo "Waiting for 10 seconds."
sleep 10
echo "KQM: $(findproc kqm)"

pushd /kqm
echo "Start a Consumer to the __consumer_offsets topic."
nohup kafka/bin/kafka-console-consumer.sh --topic __consumer_offsets \
    --bootstrap-server localhost:9092 \
    --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" \
    --from-beginning > __consumer_offsets.log 2>&1 &
echo "Waiting for 15 seconds."
sleep 15
echo "Consumer: $(findproc __consumer_offsets)"
echo "Consumer Output:"
cat __consumer_offsets.log

echo "Start a Consumer to the topic1 topic."
nohup kafka/bin/kafka-console-consumer.sh --topic topic1 \
    --bootstrap-server localhost:9092 \
    --from-beginning > topic1.log 2>&1 &
echo "Waiting for 15 seconds."
sleep 15
echo "Consumer: $(findproc topic1)"
echo "Consumer Output:"
cat topic1.log

echo "Start a Consumer to the topic2 topic."
nohup kafka/bin/kafka-console-consumer.sh --topic topic2 \
    --bootstrap-server localhost:9092 \
    --from-beginning > topic2.log 2>&1 &
echo "Waiting for 15 seconds."
sleep 15
echo "Consumer: $(findproc topic2)"
echo "Consumer Output:"
cat topic2.log

echo "Start a Consumer to the topic3 topic."
nohup kafka/bin/kafka-console-consumer.sh --topic topic3 \
    --bootstrap-server localhost:9092 \
    --from-beginning > topic3.log 2>&1 &
echo "Waiting for 15 seconds."
sleep 15
echo "Consumer: $(findproc topic3)"
echo "Consumer Output:"
cat topic3.log

echo "Start a Consumer to the topic4 topic."
nohup kafka/bin/kafka-console-consumer.sh --topic topic4 \
    --bootstrap-server localhost:9092 \
    --from-beginning > topic4.log 2>&1 &
echo "Waiting for 15 seconds."
sleep 15
echo "Consumer: $(findproc topic2)"
echo "Consumer Output:"
cat topic4.log

popd

echo "KQM Port Status: $(netstat -anlp | grep -i 8125)"
count=1
while [ $count -le 5 ]
do
echo "KQM Output until now:"
cat /kqm/kqm.log
echo "Waiting for 10 seconds."
sleep 10
((count++))
done

echo "Running tests."
pushd tests
go test -timeout 20m -v lag_test.go
