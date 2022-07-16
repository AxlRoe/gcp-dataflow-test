#!/bin/bash

gcloud projects get-iam-policy scraper-vx  \
--flatten="bindings[].members" \
--format='table(bindings.role)' \
--filter="bindings.members:data-flow-sa@scraper-vx.iam.gserviceaccount.com"


