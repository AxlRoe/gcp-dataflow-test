#!/bin/bash

gcloud redis instances create scraper-redis --size=1 --region=europe-west8 --project=scraper-vx4

echo "ip redis" 
gcloud redis instances describe scraper-redis --region=europe-west8 | grep host


