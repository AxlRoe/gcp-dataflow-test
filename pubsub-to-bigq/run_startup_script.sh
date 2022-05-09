#!/bin/bash

gcloud compute instances add-metadata $1 --metadata-from-file=startup-script=$2 --zone=europe-west1-b
