#!/bin/bash

gcloud components install beta
gcloud pubsub topics create exchange.ended.events
gcloud pubsub topics create exchange.acknowledge
gcloud pubsub topics create exchange.score
gcloud pubsub topics create discover.match
gcloud pubsub topics create discover.runner
gcloud pubsub topics create discover.watch
gcloud pubsub topics create discover.schedule
gcloud pubsub topics create scraper.orchestrator.job
gcloud pubsub topics create exchange.sample

gcloud pubsub subscriptions create exchange.acknowledge.sub --topic exchange.acknowledge
gcloud pubsub subscriptions create discover.schedule.sub --topic discover.schedule
gcloud pubsub subscriptions create scraper.orchestrator.job.sub --topic scraper.orchestrator.job
gcloud pubsub subscriptions create exchange.score.sub --topic exchange.score
gcloud pubsub subscriptions create discover.runner.sub --topic discover.runner
gcloud pubsub subscriptions create discover.match.sub --topic discover.match
gcloud pubsub subscriptions create discover.watch.sub --topic discover.watch
gcloud pubsub subscriptions create exchange.sample.sub --topic exchange.sample
gcloud pubsub subscriptions create exchange.ended.events.sub --topic exchange.ended.events


#gcloud beta pubsub topics publish myTest "hello"
#gcloud beta pubsub subscriptions create --topic myTest mySub
#gcloud beta pubsub subscriptions pull --auto-ack mySub

