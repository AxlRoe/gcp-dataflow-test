#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIAL=data-flow-sa.json
BUCKET_NAME=dump-bucket-4
PROJECT=scraper-v1-351921

virtualenv venv
source venv/bin/activate

python prepare_main.py \
--job data-preparation \
--project $PROJECT \
--region europe-west1 \
--setup_file prepare_pipeline/setup.py \
--runner DataflowRunner \
--staging_location gs://$BUCKET_NAME/staging \
--temp_location gs://dump-bucket-4/tmp/ \
--experiment use_unsupported_python_version pipeline
#--bucket $BUCKET_NAME \
#--requirements_file requirements.txt \
