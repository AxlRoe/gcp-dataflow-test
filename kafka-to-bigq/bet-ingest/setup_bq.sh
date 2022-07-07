#!/bin/bash

PROJECT=$(gcloud config get-value project)
if [[ $PORJECT != scraper-v1-351921 ]]; then
  PROJECT=scraper-v1-351921
fi

bq mk --location EU --dataset kafka_to_bigquery
bq mk --table --schema schema.json --time_partitioning_field ts kafka_to_bigquery.transactions
