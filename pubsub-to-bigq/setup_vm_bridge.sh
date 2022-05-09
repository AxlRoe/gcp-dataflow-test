#!/bin/bash

gcloud compute instances create $1 \
       	--image-family=ubuntu-2004-lts \
	--image-project=ubuntu-os-cloud \
	--scopes userinfo-email,cloud-platform \
	--machine-type=e2-small \
	--zone=europe-west1-b \
	--metadata-from-file=startup-script=$2 \
	--tags http-server,https-server 



