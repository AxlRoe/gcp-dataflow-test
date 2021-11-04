#!/bin/bash

gcloud components install beta
gcloud pubsub topics create exchange.ended.events

#gcloud beta pubsub topics publish myTest "hello"
#gcloud beta pubsub subscriptions create --topic myTest mySub
#gcloud beta pubsub subscriptions pull --auto-ack mySub

