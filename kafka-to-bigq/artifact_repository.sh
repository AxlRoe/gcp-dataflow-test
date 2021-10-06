#!/bin/bash

gcloud artifacts repositories create dataflow-repo --repository-format=docker --location=europe-west6 --description="dataflow test Docker repository"

gcloud auth configure-docker europe-west6-docker.pkg.dev

