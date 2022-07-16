#!/bin/bash

gcloud config set project scraper-vx

APIS=$(gcloud services list --available | grep -E -i 'dataflow|^stackdriver|^storage\.google|storage-api|^bigquery\.|^pubsub\.|datastore|cloudresource|^compute\.' | awk '{print $1}')

for api in $APIS; do
  echo "Enabling api " $api
  gcloud services enable $api 
done



