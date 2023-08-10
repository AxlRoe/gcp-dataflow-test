#!/bin/bash

gcloud iam service-accounts delete data-flow-sa@scraper-vx4.iam.gserviceaccount.com
gcloud iam service-accounts create data-flow-sa
gcloud projects add-iam-policy-binding scraper-vx4 --member="serviceAccount:data-flow-sa@scraper-vx4.iam.gserviceaccount.com" --role="roles/owner"
gcloud projects add-iam-policy-binding scraper-vx4 --member="serviceAccount:data-flow-sa@scraper-vx4.iam.gserviceaccount.com" --role roles/storage.admin
gcloud projects add-iam-policy-binding scraper-vx4 --member="serviceAccount:data-flow-sa@scraper-vx4.iam.gserviceaccount.com" --role roles/cloudbuild.builds.editor
gcloud projects add-iam-policy-binding scraper-vx4 --member="serviceAccount:data-flow-sa@scraper-vx4.iam.gserviceaccount.com" --role roles/artifactregistry.repoAdmin
gcloud projects add-iam-policy-binding scraper-vx4 --member="serviceAccount:data-flow-sa@scraper-vx4.iam.gserviceaccount.com" --role roles/artifactregistry.reader

gcloud iam service-accounts keys create data-flow-sa.json --iam-account=data-flow-sa@scraper-vx4.iam.gserviceaccount.com
