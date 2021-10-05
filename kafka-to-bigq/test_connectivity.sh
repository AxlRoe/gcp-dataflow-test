#!/bin/bash

export ZONE=europe-west2-a
gcloud compute instances create test-vm --zone=$ZONE --machine-type=g1-small
