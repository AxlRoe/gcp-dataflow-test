#!/bin/bash


gcloud pubsub topics delete exchange.ended.events
gcloud pubsub topics create exchange.ended.events

