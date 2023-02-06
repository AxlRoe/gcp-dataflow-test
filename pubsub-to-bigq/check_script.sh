#!/bin/bash

wait_startup_script_to_finish() {
    vm_name=$1
    vm_zone=$2
    echo -n "Wait for \"$vm_name\" startup script to exit."
    status=""
    while [[ -z "$status" ]]
    do
        sleep 3;
        echo -n "."
        logline=$(gcloud compute ssh $1 --zone=europe-west8-b --ssh-flag="-q" --command 'tac /var/log/syslog | grep -m 1 "startup-script exit status"')
	if [[ x'$logline' != x ]]; then
		echo "completed"
		status=$(echo $logline | awk '{print $NF}')
	fi
    done
    echo ""
}


wait_startup_script_to_finish $1 europe-west8-b


