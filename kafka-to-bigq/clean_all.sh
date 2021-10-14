#!/bin/bash

export PROJECT=data-flow-test-327119
export REPOSITORY=dataflow-repo
export IMAGE_NAME=ktbq-python

TEMPLATE_IMAGE="europe-west6-docker.pkg.dev/${PROJECT}/${REPOSITORY}/${IMAGE_NAME}:latest"

REGION="europe-west6-a"
BUCKET_NAME=data-flow-bucket_1
TEMPLATE_PATH=gs://$BUCKET_NAME/streaming-beam.json

gcloud dataflow jobs list \
    --filter 'NAME:streaming-beam AND STATE=Running' \
    --format 'value(JOB_ID)' \
    --region "$REGION" \
  | xargs gcloud dataflow jobs cancel --region "$REGION"


gcloud artifacts docker images delete $TEMPLATE_IMAGE

gcloud pubsub topics delete exchange.ended.events

bq rm -r -f -d $PROJECT:kafka_to_bigquery

gsutil -m rm -r gs://$BUCKET_NAME

