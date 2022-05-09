#!/bin/bash

gcloud compute networks vpc-access connectors create scraper-vpc --region europe-west1 --range 10.8.0.0/28
