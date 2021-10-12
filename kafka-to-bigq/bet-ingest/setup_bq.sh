#!/bin/bash

PROJECT=$(gcloud config get-value project)
if [[ $PORJECT != data-flow-test-327119 ]]; then
  PROJECT=data-flow-test-327119
fi

bq mk --location EU --dataset kafka_to_bigquery
bq mk --table --schema schema.json --time_partitioning_field ts kafka_to_bigquery.transactions
