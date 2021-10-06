#!/bin/bash

gcloud auth configure-docker europe-west6-docker.pkg.dev
export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/data-flow-sa.json
export BUCKET_NAME=gs://data-flow-bucket_1
#gsutil mb -p data-flow-test-327119 -c NEARLINE -l europe-west6 -b on gs://data-flow-bucket_1
gsutil cp spec.json $BUCKET_NAME/images/


if [ ! -d ./DataflowTemplates ]; then
	git clone https://github.com/GoogleCloudPlatform/DataflowTemplates
fi

cd ./DataflowTemplates/v2
export PROJECT=data-flow-test-327119
export REPOSITORY=dataflow-repo
export IMAGE_NAME=ktbq-test
export TARGET_GCR_IMAGE=europe-west6-docker.pkg.dev/${PROJECT}/${REPOSITORY}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=kafka-to-bigquery
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
mvn clean package -DskipTests -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC} \
                  -am -pl ${TEMPLATE_MODULE}

export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/spec.json

export TOPICS=exchange.samples
export BOOTSTRAP=10.172.0.4:9092

export OUTPUT_TABLE=${PROJECT}:kafka_to_bigquery.transactions
export JS_PATH=${BUCKET_NAME}/my_function.js
export JS_FUNC_NAME=transform
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"

gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=europe-west6 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters ^~^outputTableSpec=${OUTPUT_TABLE}~inputTopics=${TOPICS}~javascriptTextTransformGcsPath=${JS_PATH}~javascriptTextTransformFunctionName=${JS_FUNC_NAME}~bootstrapServers=${BOOTSTRAP}

cd -



