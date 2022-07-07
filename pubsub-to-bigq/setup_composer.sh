#!/bin/bash

gcloud composer environments create scraper-v1-cc-env \
    --location europe-west1 \
    --image-version composer-1.19.1-airflow-2.2.5
