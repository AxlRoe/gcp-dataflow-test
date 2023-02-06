#!/bin/bash

gcloud sql instances create $1 --database-version=POSTGRES_11 --cpu=1 --memory=4GB --region=europe-west8 --root-password=postgres
#gcloud sql users set-password postgres --instance=scraper-db --password=postgres

