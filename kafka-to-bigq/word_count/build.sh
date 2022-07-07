#!/bin/bash

KAFKA_ADDRESS=$(gcloud compute instances describe kafka-1-kafka-vm-0 --zone=europe-west6-a --format="yaml(networkInterfaces)" | grep natIP | awk '{print $2}')

TEMPLATE_NAME=Word_Count
REGION="europe-west6-a"
export GOOGLE_APPLICATION_CREDENTIAL=$(pwd)/data-flow-sa.json
BUCKET_NAME=data-flow-bucket_1
PROJECT=scraper-v1-351921

#export TEMPLATE_IMAGE="europe-west6-docker.pkg.dev/${PROJECT}/${REPOSITORY}/${IMAGE_NAME}:latest"
# Build the image into Container Registry, this is roughly equivalent to:
#   gcloud auth configure-docker
#   docker image build -t $TEMPLATE_IMAGE .
#   docker push $TEMPLATE_IMAGE
#gcloud builds submit --tag "$TEMPLATE_IMAGE" .

virtualenv env
source env/bin/activate
pip install -U -r requirements.txt

#export TEMPLATE_PATH="gs://$BUCKET_NAME/streaming-beam.json"

python wordcount.py \
    --template true \
    --runner DataflowRunner \
    --project $PROJECT \
    --staging_location gs://$BUCKET_NAME/staging \
    --temp_location gs://$BUCKET_NAME/temp \
    --template_location gs://$BUCKET_NAME/templates/$TEMPLATE_NAME \
    --region $REGION

python main.py \
  --project $PROJECT \
  --job wordcount-$(date +'%Y%m%d-%H%M%S') \
  --template gs://$BUCKET_NAME/templates/$TEMPLATE_NAME \
  --inputFile gs://apache-beam-samples/shakespeare/kinglear.txt \
  --output gs://$BUCKET_NAME/wordcount/outputs

#gcloud dataflow flex-template build $TEMPLATE_PATH \
#  --image "$TEMPLATE_IMAGE" \
#  --sdk-language "PYTHON" \
#  --metadata-file "metadata.json"
#
#export REGION="europe-west6-a"
#
## Run the Flex Template.
#gcloud dataflow flex-template run "streaming-beam-`date +%Y%m%d-%H%M%S`" \
#    --template-file-gcs-location "$TEMPLATE_PATH" \
#    --parameters input_subscription="projects/$PROJECT/subscriptions/$SUBSCRIPTION" \
#    --parameters output_table="$PROJECT:$DATASET.$TABLE" \
#    --region "$REGION"
