#!/bin/bash

PAYLOAD=$(jq -c . message.json)
gcloud beta pubsub topics publish discover.mr --message "$PAYLOAD"

