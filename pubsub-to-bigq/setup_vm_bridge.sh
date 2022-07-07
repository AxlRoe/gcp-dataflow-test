#!/bin/bash

TMPL=$3
PROJECT=$2
#ADDR=$4

cp $TMPL script.sh
sed -i "s/<PROJECT_PLACEHOLDER>/"$PROJECT"/g" script.sh
echo "script is "
cat script.sh

gcloud compute instances create $1 \
       	--image-family=ubuntu-2004-lts \
	--image-project=ubuntu-os-cloud \
	--scopes userinfo-email,cloud-platform \
	--machine-type=e2-small \
	--zone=europe-west1-b \
	--metadata-from-file=startup-script=script.sh \
	--tags http-server,https-server 
	
#--address=$ADDR \



