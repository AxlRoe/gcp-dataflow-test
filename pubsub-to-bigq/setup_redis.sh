#!/bin/bash

gcloud redis instances create scraper-redis --size=1 --region=europe-west1 --project=scraper-vx 

echo "ip redis" 
gcloud redis instances describe scraper-redis --region=europe-west1 | grep host


