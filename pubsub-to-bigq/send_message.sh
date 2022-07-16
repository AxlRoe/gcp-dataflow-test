#!/bin/bash

PAYLOAD=$(jq -c . msg.json)
gcloud beta pubsub topics publish test --message "$PAYLOAD"

