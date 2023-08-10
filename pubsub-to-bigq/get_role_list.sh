#!/bin/bash

gcloud projects get-iam-policy scraper-vx4  \
--flatten="bindings[].members" \
--format='table(bindings.role)' \
--filter="bindings.members:data-flow-sa@scraper-vx4.iam.gserviceaccount.com"


