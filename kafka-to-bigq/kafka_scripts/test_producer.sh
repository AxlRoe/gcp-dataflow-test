#!/bin/bash

ADDRESS=$(gcloud compute instances describe kafka-1-kafka-vm-0 --zone=europe-west6-a --format="yaml(networkInterfaces)" | grep natIP | awk '{print $2}')
(echo -n "1|"; cat message.json | jq . -c) | /usr/local/bin/kafka/./kafka-console-producer.sh \
--broker-list $ADDRESS:9092 \
--topic exchange.samples \
--property "parse.key=true" \
--property "key.separator=|"
