#!/bin/bash

BUCKET=$(gcloud alpha storage ls --project=scraper-vx4 | grep scraper)
gcloud composer environments delete scraper-v1-cc-env --location europe-west1

gsutil rm -r $BUCKET
DISKS=$(gcloud compute disks list | grep scraper)
for disk in $DISKS; do
	gcloud compute disks delete $disk --region=europe-west1
done


