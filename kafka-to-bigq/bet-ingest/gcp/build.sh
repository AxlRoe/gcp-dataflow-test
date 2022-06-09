#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIAL=data-flow-sa.json
BUCKET_NAME=dump-bucket-4
PROJECT=scraper-v1-351921

virtualenv venv
source venv/bin/activate

python pipeline_gcp.py \
--job data-preparation \
--bucket $BUCKET_NAME \
--region europe-west1 \
--runner DataflowRunner \
--requirements_file requirements.txt
--project $PROJECT \
--temp_location gs://dump-bucket-4/tmp/ \
--experiment use_unsupported_python_version pipeline \
--staging_location gs://$BUCKET_NAME/staging

