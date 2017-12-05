#!/bin/bash

# NOTE: Before using any of these commands, make sure you are in the kafka dir.

# Start Kafka Server:
nohup bin/kafka-server-start.sh config/server.properties > running.log 2>&1 &

# Create a Kafka Topic:
bin/kafka-topics.sh --create --topic topic_1 --zookeeper localhost:2181 --partitions 4 --replication-factor 4

# Delete a Kafka Topic:
bin/kafka-topics.sh --delete --topic topic_1 --zookeeper localhost:2181

# List all available Kafka Topics (Old):
bin/kafka-topics.sh  --list --zookeeper localhost:2181

# Create a Kafka Producer:
bin/kafka-console-producer.sh --topic topic_1 --broker-list localhost:9092

# Create a Kafka Non-Zookeeper (New) Consumer:
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic_1 --from-beginning

# List Non-Zookeeper (New) Consumer Groups (including the ones created
# when a new consumer is created):
bin/kafka-consumer-groups.sh --list --bootstrap-server localhost:9092

# Create a Kafka Zookeeper (Old) Consumer:
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic topic_1 --from-beginning

# List Non-Zookeeper (Old) Consumer Groups (including the ones created
# when a new consumer is created):
bin/kafka-consumer-groups.sh --list --zookeeper localhost:2181

# Subscribe and read from __consumer_offsets Topic (New):
bin/kafka-console-consumer.sh --topic __consumer_offsets --bootstrap-server localhost:9092 --formatter "kafka.coordinator.GroupMetadataManager\$OffsetsMessageFormatter" --from-beginning

# Subscribe and read from __consumer_offsets Topic (Old):
bin/kafka-console-consumer.sh --topic __consumer_offsets --zookeeper localhost:2181 --formatter "kafka.coordinator.GroupMetadataManager\$OffsetsMessageFormatter" --from-beginning
