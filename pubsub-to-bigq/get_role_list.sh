#!/bin/bash

gcloud projects get-iam-policy scraper-vx3  \
--flatten="bindings[].members" \
--format='table(bindings.role)' \
--filter="bindings.members:data-flow-sa@scraper-vx3.iam.gserviceaccount.com"


