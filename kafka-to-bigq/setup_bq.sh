#!/bin/bash

bq mk --location EU --dataset kafka_to_bigquery
bq mk --table --schema ./bet-ingestor/src/main/resources/schema.json --time_partitioning_field ts kafka_to_bigquery.transactions
