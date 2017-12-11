#!/bin/bash
#
# This is a script to setup and start zookeeper and kafka.
#

# Update dependencies
apt-get update && apt-get install -y zookeeper zookeeperd net-tools wget

# Install Kafka
wget http://redrockdigimark.com/apachemirror/kafka/0.11.0.2/kafka_2.11-0.11.0.2.tgz
tar xvf kafka_2.11-0.11.0.2.tgz
mv kafka_2.11-0.11.0.2 kafka

# Common functions
function find_proc() {
	ps aux | grep -i "$1" | grep -v grep
}

# Start Zookeeper
echo "Starting Zookeeper."
service zookeeper start
echo "Waiting for 10 seconds."
sleep 10
echo "Zookeeper: $(find_proc zookeeper)"
echo "Zookeeper Log File:"
tail -n 5 /var/log/zookeeper/zookeeper.log

# Start Kafka
echo "Starting Kafka."
nohup kafka/bin/kafka-server-start.sh kafka/config/server.properties >kafka.log \
    2>&1 &
echo "Waiting for 10 seconds."
sleep 10
echo "Kafka: $(find_proc kafka)"
echo "Kafka Log File:"
tail -n 5 kafka.log

# Tail for logs from Zookeeper and Kafka.
echo "Tailing for Zookeeper and Kafka logs."
tail -f /var/log/zookeeper/zookeeper.log kafka.log
