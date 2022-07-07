#!/bin/bash

REDIS_HOST=$(gcloud redis instances describe scraper-redis --format='get(host)'  --region=europe-west1)
gcloud compute ssh redis-vm --zone=europe-west1-b -- -N -L 6379:$REDIS_HOST:6379


