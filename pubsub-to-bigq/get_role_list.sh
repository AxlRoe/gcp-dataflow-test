#!/bin/bash

gcloud projects get-iam-policy scraper-v1-351921  \
--flatten="bindings[].members" \
--format='table(bindings.role)' \
--filter="bindings.members:data-flow-sa@scraper-v1-351921.iam.gserviceaccount.com"


