#!/bin/bash

wait_startup_script_to_finish() {
    vm_name=$1
    vm_zone=$2
    echo -n "Wait for \"$vm_name\" startup script to exit."
    status=""
    while [[ x"$status" == "x" ]]
    do
        sleep 3;
	echo -n "."
        logline=$(gcloud compute ssh $1 --zone=europe-west8-b --ssh-flag="-q" --command 'tac /var/log/syslog | grep -m 1 "startup-script exit status"')
        if [[ $logline == *"startup-script exit"* ]]; then
                echo "completed"
                status="stop"
        fi
    done
    echo ""
}

#gcloud compute addresses create addr-discover-vm --region=europe-west8
#gcloud compute addresses create addr-orc-vm --region=europe-west8
#gcloud compute addresses create addr-monitor-vm --region=europe-west8

#LAPTOP_ADDR=$(curl ifconfig.me)
#DISCO_ADDR=$(gcloud compute addresses describe addr-discover-vm --region=europe-west8 | grep address: | awk -F' ' '{print $2}')
#ORC_ADDR=$(gcloud compute addresses describe addr-orc-vm --region=europe-west8 | grep address: | awk -F' ' '{print $2}')
#MONITOR_ADDR=$(gcloud compute addresses describe addr-monitor-vm --region=europe-west8 | grep address: | awk -F' ' '{print $2}')

GITHUB='AxlRoe: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'
DB=104.199.35.248
REDIS=10.67.146.251

./setup_vm_bridge.sh orc-vm bet-scraper-orchestrator setup_java_vm.tmpl $GITHUB "" $DB $REDIS
./setup_vm_bridge.sh discover-vm bet-discover setup_java_vm.tmpl $GITHUB "" $DB $REDIS
./setup_vm_bridge.sh monitor-vm bet-odds-monitor setup_python_vm.tmpl $GITHUB bet $DB $REDIS
#./setup_vm_bridge.sh bff-vm bet-bff setup_python_vm.tmpl $GITHUB "" $DB $REDIS
./setup_vm_bridge.sh recom-vm bet-recommender setup_python_vm.tmpl $GITHUB "" $DB $REDIS

LAPTOP_ADDR=$(curl ifconfig.me)
DISCO_ADDR=$(gcloud compute instances describe discover-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west8-b)
ORC_ADDR=$(gcloud compute instances describe orc-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west8-b)
MONITOR_ADDR=$(gcloud compute instances describe monitor-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west8-b)
RECOM_ADDR=$(gcloud compute instances describe recom-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west8-b)
#BFF_ADDR=$(gcloud compute instances describe bff-vm --format='get(networkInterfaces[0].accessConfigs[0].natIP)' --zone=europe-west8-b)

gcloud compute addresses create addr-discover-vm --addresses=$DISCO_ADDR --region=europe-west8
gcloud compute addresses create addr-recom-vm --addresses=$RECOM_ADDR --region=europe-west8
gcloud compute addresses create addr-orc-vm --addresses=$ORC_ADDR --region=europe-west8
gcloud compute addresses create addr-monitor-vm --addresses=$MONITOR_ADDR --region=europe-west8
#gcloud compute addresses create addr-bff-vm --addresses=$BFF_ADDR --region=europe-west8

#gcloud sql instances patch scraper-db-3 --authorized-networks=$LAPTOP_ADDR,$DISCO_ADDR,$ORC_ADDR,$MONITOR_ADDR,$BFF_ADDR,$RECOM_ADDR --quiet
gcloud sql instances patch scraper-db-3 --authorized-networks=$LAPTOP_ADDR,$DISCO_ADDR,$ORC_ADDR,$MONITOR_ADDR,$RECOM_ADDR --quiet
#gcloud sql instances patch scraper-db-3 --authorized-networks=$LAPTOP_ADDR,$DISCO_ADDR --quiet

wait_startup_script_to_finish discover-vm europe-west8-b
wait_startup_script_to_finish recom-vm europe-west8-b
wait_startup_script_to_finish monitor-vm europe-west8-b
wait_startup_script_to_finish orc-vm europe-west8-b
