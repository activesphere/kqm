#!/bin/sh

# Download the latest Toxiproxy binaries.
wget -O toxiproxy https://github.com/Shopify/toxiproxy/releases/download/v2.1.2/toxiproxy-server-linux-amd64
wget -O toxiproxy-cli https://github.com/Shopify/toxiproxy/releases/download/v2.1.2/toxiproxy-cli-linux-amd64
chmod +x toxiproxy toxiproxy-cli

# Start the toxiproxy server.
nohup ./toxiproxy > toxiproxy.log 2>&1 &
sleep 5

# Create a Proxy for Kafka.
echo "Creating a proxy for Kafka at port 9092."
./toxiproxy-cli create kafka --listen 0.0.0.0:9092 --upstream kafka:9092

# Create a Proxy for Zookeeper.
echo "Creating a proxy for Zookeeper at port 2181."
./toxiproxy-cli create zookeeper --listen 0.0.0.0:2181 --upstream kafka:2181

# Tail the Toxiproxy logs.
tail -f toxiproxy.log
