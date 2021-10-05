#!/bin/bash


(echo -n "1|"; cat message.json | jq . -c) | /opt/kafka/bin/kafka-console-producer.sh \
--broker-list $1:9092 \
--topic exchange.samples \
--property "parse.key=true" \
--property "key.separator=|"
