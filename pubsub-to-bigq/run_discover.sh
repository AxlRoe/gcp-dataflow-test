#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS="/home/io/gcloud_home/gcp-dataflow-test/pubsub-to-bigq/data-flow-sa.json"
export REDIS_HOST="127.0.0.1"
export DB_CONN="scraper-v1:europe-west1:scraper-db-6"
export EXCHANGE_ADDRESS="localhost"

java -jar /home/io/gcloud_home/gcp-app-test/bet-discover/target/bet-discover-0.0.1-SNAPSHOT.jar


