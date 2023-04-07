#!/bin/bash

if [[ x$1 == 'x' ]]; then
   echo "missing payload to send"	
   exit 1
fi

if [[ x$2 == 'x' ]]; then
   echo "missing topic"
   exit 1
fi

PAYLOAD=$(jq -c . $1)
gcloud beta pubsub topics publish $2 --message "$PAYLOAD"

