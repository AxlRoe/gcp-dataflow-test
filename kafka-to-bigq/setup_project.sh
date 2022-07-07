#!/bin/bash

gcloud config set project scraper-v1-351921

APIS=$(gcloud services list --available | grep -E -i 'cloudbuild|containerregistry|dataflow|^stackdriver|^storage\.google|storage-api|^bigquery\.|^pubsub\.|datastore|cloudresource|^compute\.' | awk '{print $1}')

for api in $APIS; do
  echo "Enabling api " $api
  gcloud services enable $api 
done



