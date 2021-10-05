#!/bin/bash


ADDRESS=34.65.59.41
echo "Cleaning topics"
TOPICS=$(/usr/local/bin/kafka/./kafka-topics.sh --bootstrap-server $ADDRESS:9092 --list); for t in $TOPICS; do /usr/local/bin/kafka/./kafka-topics.sh --bootstrap-server $ADDRESS:9092 --topic $t --delete; done

echo "create topics"
/usr/local/bin/kafka/./kafka-topics.sh --bootstrap-server $ADDRESS:9092 --create --replication-factor 1 --partitions 1 --topic exchange.acknowledge
/usr/local/bin/kafka/./kafka-topics.sh --bootstrap-server $ADDRESS:9092 --create --replication-factor 1 --partitions 1 --topic exchange.samples


