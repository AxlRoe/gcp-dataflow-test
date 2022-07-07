#!/bin/bash

#gcloud compute addresses create addr-discover-vm --region=europe-west1
#gcloud compute addresses create addr-orc-vm --region=europe-west1
#gcloud compute addresses create addr-monitor-vm --region=europe-west1

#LAPTOP_ADDR=$(curl ifconfig.me)
#DISCO_ADDR=$(gcloud compute addresses describe addr-discover-vm --region=europe-west1 | grep address: | awk -F' ' '{print $2}')
#ORC_ADDR=$(gcloud compute addresses describe addr-orc-vm --region=europe-west1 | grep address: | awk -F' ' '{print $2}')
#MONITOR_ADDR=$(gcloud compute addresses describe addr-monitor-vm --region=europe-west1 | grep address: | awk -F' ' '{print $2}')

./setup_vm_bridge.sh orc-vm bet-scraper-orchestrator setup_java_vm.tmpl #$ORC_ADDR
./setup_vm_bridge.sh discover-vm bet-discover setup_java_vm.tmpl #$DISCO_ADDR
./setup_vm_bridge.sh monitor-vm bet-odds-monitor setup_python_vm.tmpl #$MONITOR_ADDR

LAPTOP_ADDR=$(curl ifconfig.me)
DISCO_ADDR=$(gcloud compute instances describe discover-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west1-b)
ORC_ADDR=$(gcloud compute instances describe orc-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west1-b)
MONITOR_ADDR=$(gcloud compute instances describe monitor-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west1-b)

gcloud compute addresses create addr-discover-vm --addresses=$DISCO_ADDR --region=europe-west1
gcloud compute addresses create addr-orc-vm --addresses=$ORC_ADDR --region=europe-west1
gcloud compute addresses create addr-monitor-vm --addresses=$MONITOR_ADDR --region=europe-west1

gcloud sql instances patch scraper-db-3 --authorized-networks=$LAPTOP_ADDR,$DISCO_ADDR,$ORC_ADDR,$MONITOR_ADDR --quiet

#./setup_vm_bridge.sh recommender-vm bet-recommender setup_python_vm.tmpl
#./setup_vm_bridge.sh dp-vm bet-ui-data-provider setup_python_vm.tmpl
#./setup_vm_bridge.sh ui-vm bet-ui setup_node_vm.tmpl
