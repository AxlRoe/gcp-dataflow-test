#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIAL=$(pwd)/data-flow-sa.json
BUCKET_NAME=dump-bucket-3
PROJECT=scraper-v1

virtualenv env
source env/bin/activate
pip install -U -r requirements.txt

python pipeline.py \
--bucket $BUCKET_NAME
--region europe-west1 \
--runner DataflowRunner \
--project scraper-v1 \
--temp_location gs://$BUCKET_NAME/tmp/ \
--staging_location gs://$BUCKET_NAME/staging