#!/bin/bash

PAYLOAD=$(jq -c . msg.json)
gcloud beta pubsub topics publish exchange.sample --message "$PAYLOAD"

