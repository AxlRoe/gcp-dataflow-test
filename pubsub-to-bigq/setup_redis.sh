#!/bin/bash

gcloud redis instances create scraper-redis --size=1 --region=europe-west1 --project=scraper-v1 

echo "ip redis" 
gcloud redis instances describe scraper-redis --region=europe-west1 | grep host


