#!/bin/bash

if [[ x$1 == "x" ]]; then
 echo "missing reserved range name"
 exit 1
fi


gcloud services vpc-peerings connect --service=servicenetworking.googleapis.com --ranges=$1 --network=default --project=scraper-vx3


