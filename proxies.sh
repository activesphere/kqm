#!/bin/sh

# Create a Proxy for Kafka.
echo "Creating a proxy for Kafka at port 9092."
./go/bin/toxiproxy-cli create kafka --listen 0.0.0.0:9092 --upstream kafka:9092

# Create a Proxy for Zookeeper.
echo "Creating a proxy for Zookeeper at port 2181."
./go/bin/toxiproxy-cli create zookeeper --listen 0.0.0.0:2181 --upstream zookeeper:9092
