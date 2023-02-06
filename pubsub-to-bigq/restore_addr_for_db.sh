#!/bin/bash

LAPTOP_ADDR=$(curl ifconfig.me)
#DISCO_ADDR=$(gcloud compute instances describe discover-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west8-b)
#ORC_ADDR=$(gcloud compute instances describe orc-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west8-b)
#MONITOR_ADDR=$(gcloud compute instances describe monitor-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west8-b)
#RECOM_ADDR=$(gcloud compute instances describe recom-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west8-b)
#BFF_ADDR=$(gcloud compute instances describe bff-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west8-b)

#gcloud sql instances patch scraper-db-3 --authorized-networks=$LAPTOP_ADDR,$DISCO_ADDR,$ORC_ADDR,$MONITOR_ADDR,$BFF_ADDR,$RECOM_ADDR --quiet
#gcloud sql instances patch scraper-db-3 --authorized-networks=$LAPTOP_ADDR,$DISCO_ADDR,$ORC_ADDR,$MONITOR_ADDR,$RECOM_ADDR --quiet
gcloud sql instances patch scraper-db --authorized-networks=$LAPTOP_ADDR --quiet
#gcloud sql instances patch scraper-db-3 --authorized-networks=$LAPTOP_ADDR,$DISCO_ADDR --quiet


