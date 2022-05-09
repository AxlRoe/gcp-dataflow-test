#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIAL=data-flow-sa.json
BUCKET_NAME=dump-bucket-3
PROJECT=scraper-v1


python pipeline_gcp.py \
--job data-preparation \
--bucket $BUCKET_NAME \
--region europe-west1 \
--runner DirectRunner \
--project scraper-v1 \
--temp_location gs://dump-bucket-3/tmp/ \
--experiment use_unsupported_python_version pipeline \
--staging_location gs://$BUCKET_NAME/staging

