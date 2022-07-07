#!/bin/bash

gcloud config set project scraper-v1-351921

APIS=$(gcloud services list --available | grep -E -i 'cloudbuild|containerregistry|dataflow|^stackdriver|^storage\.google|storage-api|^bigquery\.|^pubsub\.|datastore|cloudresource|^compute\.' | awk '{print $1}')

for api in $APIS; do
  echo "Enabling api " $api
  gcloud services enable $api 
done

gcloud auth configure-docker europe-west6-docker.pkg.dev
gsutil mb -p scraper-v1-351921 -c NEARLINE -l europe-west6 -b on gs://data-flow-bucket_1

gcloud iam service-accounts delete data-flow-sa@scraper-v1-351921.iam.gserviceaccount.com
gcloud iam service-accounts create data-flow-sa
gcloud projects add-iam-policy-binding scraper-v1-351921 --member="serviceAccount:data-flow-sa@scraper-v1-351921.iam.gserviceaccount.com" --role="roles/owner"
gcloud projects add-iam-policy-binding scraper-v1-351921 --member="serviceAccount:data-flow-sa@scraper-v1-351921.iam.gserviceaccount.com" --role roles/storage.admin 
gcloud projects add-iam-policy-binding scraper-v1-351921 --member="serviceAccount:data-flow-sa@scraper-v1-351921.iam.gserviceaccount.com" --role roles/artifactregistry.repoAdmin
gcloud projects add-iam-policy-binding scraper-v1-351921 --member="serviceAccount:data-flow-sa@scraper-v1-351921.iam.gserviceaccount.com" --role roles/artifactregistry.reader

gcloud iam service-accounts keys create data-flow-sa.json --iam-account=data-flow-sa@scraper-v1-351921.iam.gserviceaccount.com

gcloud config set builds/use_kaniko True
gcloud config set builds/kaniko_cache_ttl 6

