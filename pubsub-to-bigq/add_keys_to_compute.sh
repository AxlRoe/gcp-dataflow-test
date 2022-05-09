#!/bin/bash

gcloud compute os-login ssh-keys add --key-file=/home/io/.ssh/key_for_gcp.txt.pub --project=scraper-v1 --ttl=30d
