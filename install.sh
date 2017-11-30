#!/bin/bash

# Update dependencies
apt-get clean && apt-get update
apt-get install -y python zookeeper zookeeperd wget software-properties-common git

# Install Go
add-apt-repository ppa:longsleep/golang-backports
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
