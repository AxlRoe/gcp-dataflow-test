#!/bin/bash

gcloud iam service-accounts create data-flow-sa
gcloud projects add-iam-policy-binding data-flow-test-327119 --member="serviceAccount:data-flow-sa@data-flow-test-327119.iam.gserviceaccount.com" --role="roles/owner"

gcloud projects add-iam-policy-binding data-flow-test-327119 --member="serviceAccount:data-flow-sa@data-flow-test-327119.iam.gserviceaccount.com" --role roles/storage.admin 
gcloud projects add-iam-policy-binding data-flow-test-327119 --member="serviceAccount:data-flow-sa@data-flow-test-327119.iam.gserviceaccount.com" --role roles/artifactregistry.repoAdmin

gcloud iam service-accounts keys create data-flow-sa.json --iam-account=data-flow-sa@data-flow-test-327119.iam.gserviceaccount.com
