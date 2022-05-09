#!/bin/bash


gcloud compute instances delete $1 --zone=europe-west1-b --quiet
