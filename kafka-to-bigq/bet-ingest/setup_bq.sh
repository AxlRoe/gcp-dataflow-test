#!/bin/bash

PROJECT=$(gcloud config get-value project)
if [[ $PORJECT != scraper-vx ]]; then
  PROJECT=scraper-vx
fi

bq mk --location EU --dataset kafka_to_bigquery
bq mk --table --schema schema.json --time_partitioning_field ts kafka_to_bigquery.transactions
