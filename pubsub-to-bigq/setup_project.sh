#!/bin/bash

gcloud config set project scraper-vx3

APIS=$(gcloud services list --available | awk '{print $NF}' | grep -E -i '^servicenetworking.|^artifactregistry\.|^cloudbuild\.|^sourcerepo\.|^container\.|^cloudfunction|dataflow|^stackdriver|^storage\.google|storage-api|^bigquery\.|^pubsub\.|datastore|cloudresource|^compute\.')

for api in $APIS; do
  echo "Enabling api " $api
  gcloud services enable $api 
done



