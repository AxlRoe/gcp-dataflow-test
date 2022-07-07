#!/bin/bash

gcloud composer environments storage dags import \
    --environment scraper-v1-cc-env \
    --location europe-west1 \
    --source="dag_ml.py"