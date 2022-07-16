#!/bin/bash

gcloud pubsub topics delete exchange.acknowledge
gcloud pubsub topics delete exchange.score
gcloud pubsub topics delete discover.mr
gcloud pubsub topics delete discover.schedule
gcloud pubsub topics delete scraper.orchestrator.job
gcloud pubsub topics delete exchange.sample
gcloud pubsub topics delete exchange.ended.events

gcloud pubsub subscriptions delete exchange.acknowledge.sub
gcloud pubsub subscriptions delete discover.schedule.sub 
gcloud pubsub subscriptions delete scraper.orchestrator.job.sub 
gcloud pubsub subscriptions delete exchange.score.sub 
gcloud pubsub subscriptions delete discover.mr.sub 
gcloud pubsub subscriptions delete exchange.sample.sub 
gcloud pubsub subscriptions delete exchange.sample.validator.sub 
gcloud pubsub subscriptions delete exchange.ended.events.sub 


