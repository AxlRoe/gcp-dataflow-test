#!/bin/bash

gcloud services vpc-peerings delete \
    --service=servicenetworking.googleapis.com \
    --network=default
