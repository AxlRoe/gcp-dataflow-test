#!/bin/bash

gcloud artifacts repositories create tekton-test-repo --repository-format=docker --location=europe-west2 --description="Tekton Docker repository"

gcloud auth configure-docker europe-west2-docker.pkg.dev

