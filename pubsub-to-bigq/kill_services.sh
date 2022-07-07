#!/bin/bash

gcloud compute ssh root@monitor-vm \
	--command="chmod +x kill_python.sh; ./kill_python.sh" \
	--zone=europe-west1-b

gcloud compute ssh root@orc-vm \
	--command="chmod +x kill_java.sh; ./kill_java.sh" \
	--zone=europe-west1-b

gcloud compute ssh root@discover-vm \
	--command="chmod +x kill_java.sh; ./kill_java.sh" \
	--zone=europe-west1-b


