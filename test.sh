#!/bin/bash
#
# This is a script to run the tests and return an exit code
# (non-zero is error).
#

function find_proc() {
	ps aux | grep -i "$1" | grep -v grep
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
	nohup kafka/bin/kafka-console-consumer.sh --topic "$1" \
		--bootstrap-server kafka:9092 \
		--formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" \
		--from-beginning > "$1".log 2>&1 &
	echo "Waiting for 10 seconds."
	sleep 10
	echo "Consumer: $(find_proc "$1")"
	echo "Consumer Output:"
	tail -n 5 "$1.log"
	popd
}

function create_topics() {
	topicIndex=1
	while [ $topicIndex -le "$1" ]
	do
		kafka/bin/kafka-topics.sh --create --topic topic$topicIndex \
			--zookeeper kafka:2181 \
			--partitions 4 --replication-factor 1
		((topicIndex++))
	done
}

echo "Installing dependencies."
./install.sh > /dev/null 2>&1
echo "Dependency installation complete."

echo "Creating Kafka Topics."
create_topics 3

echo "Setting up KQM."
mkdir -p /kqm/go/src/github.com/activesphere
pushd /kqm/go/src/github.com/activesphere
echo "Current Branch: $1"
git clone --depth=50 --branch="$1" https://github.com/activesphere/kqm

echo "Building KQM."
export GOPATH=/kqm/go
pushd /kqm/go/src/github.com/activesphere/kqm
go build

start_consumer __consumer_offsets

echo "Starting KQM."
nohup ./kqm --log-level=2 \
	--interval=1 \
	--statsd-addr localhost:8125 \
	--statsd-prefix prefix_demo \
	kafka:9092 > /kqm/kqm.log 2>&1 &
echo "Waiting for 10 seconds."
sleep 10
echo "KQM: $(find_proc kqm)"

log_kqm_status

echo "Running tests."
pushd tests
go test -timeout 20m -v lag_test.go
