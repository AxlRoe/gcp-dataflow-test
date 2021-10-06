#!/bin/bash


ADDRESS=$(gcloud compute instances describe kafka-1-kafka-vm-0 --zone=europe-west6-a --format="yaml(networkInterfaces)" | grep natIP | awk '{print $2}')
echo "Cleaning topics"
TOPICS=$(/usr/local/bin/kafka/./kafka-topics.sh --bootstrap-server $ADDRESS:9092 --list); for t in $TOPICS; do /usr/local/bin/kafka/./kafka-topics.sh --bootstrap-server $ADDRESS:9092 --topic $t --delete; done

echo "create topics"
/usr/local/bin/kafka/./kafka-topics.sh --bootstrap-server $ADDRESS:9092 --create --replication-factor 1 --partitions 1 --topic exchange.acknowledge
/usr/local/bin/kafka/./kafka-topics.sh --bootstrap-server $ADDRESS:9092 --create --replication-factor 1 --partitions 1 --topic exchange.samples


