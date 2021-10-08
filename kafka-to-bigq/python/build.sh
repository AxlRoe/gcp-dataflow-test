#!/bin/bash

KAFKA_ADDRESS=$(gcloud compute instances describe kafka-1-kafka-vm-0 --zone=europe-west6-a --format="yaml(networkInterfaces)" | grep natIP | awk '{print $2}')

export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/data-flow-sa.json
export BUCKET_NAME=data-flow-bucket_1
export PROJECT=data-flow-test-327119
export REPOSITORY=dataflow-repo
export IMAGE_NAME=ktbq-python

export TEMPLATE_IMAGE="europe-west6-docker.pkg.dev/${PROJECT}/${REPOSITORY}/${IMAGE_NAME}:latest"
# Build the image into Container Registry, this is roughly equivalent to:
#   gcloud auth configure-docker
#   docker image build -t $TEMPLATE_IMAGE .
#   docker push $TEMPLATE_IMAGE
gcloud builds submit --tag "$TEMPLATE_IMAGE" .

export TEMPLATE_PATH="gs://$BUCKET_NAME/streaming-beam.json"
gcloud dataflow flex-template build $TEMPLATE_PATH \
  --image "$TEMPLATE_IMAGE" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata.json"

export REGION="europe-west6-a"

# Run the Flex Template.
gcloud dataflow flex-template run "streaming-beam-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input_subscription="projects/$PROJECT/subscriptions/$SUBSCRIPTION" \
    --parameters output_table="$PROJECT:$DATASET.$TABLE" \
    --region "$REGION"
