#!/bin/bash

PAYLOAD=$(jq -c . ps_notif.json)
gcloud beta pubsub topics publish exchange.ended.events --message "$PAYLOAD" 

