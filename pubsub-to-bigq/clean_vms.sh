#!/bin/bash

gcloud compute addresses delete addr-discover-vm --region=europe-west1 --quiet
gcloud compute addresses delete addr-orc-vm --region=europe-west1 --quiet
gcloud compute addresses delete addr-monitor-vm --region=europe-west1 --quiet

./clean_vm.sh discover-vm
./clean_vm.sh orc-vm
./clean_vm.sh monitor-vm


