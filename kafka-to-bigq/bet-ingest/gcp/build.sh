#!/bin/bash

export DB_DRIVER=postgresql+pg8000
export GOOGLE_APPLICATION_CREDENTIAL=data-flow-sa.json
BUCKET_NAME=dump-bucket-4
PROJECT=scraper-vx
DB_HOST=104.199.35.248

virtualenv venv
source venv/bin/activate

python prepare_main.py \
--job data-preparation \
--project $PROJECT \
--region europe-west1 \
--setup_file ./setup.py \
--runner DataflowRunner \
--db_url postgresql://postgres:postgres@$DB_HOST:5432/postgres \
--staging_location gs://$BUCKET_NAME/staging \
--temp_location gs://$BUCKET_NAME/tmp/ \
--experiment use_unsupported_python_version pipeline
#--bucket $BUCKET_NAME \
#--requirements_file requirements.txt \
