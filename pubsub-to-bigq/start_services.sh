#!/bin/bash

gcloud compute ssh root@monitor-vm \
	--command="cd /root/bet-odds-monitor; source venv/bin/./activate; nohup compute/./start_gcp.sh > /root/run.log 2>&1 &" \
	--zone=europe-west1-b

gcloud compute ssh root@orc-vm \
	--command="cd /root/bet-scraper-orchestrator/src/main/compute; nohup ./start_gcp.sh > /root/scraper-logs/run.log 2>&1 &" \
	--zone=europe-west1-b

gcloud compute ssh root@discover-vm \
	--command="cd /root/bet-discover/src/main/compute/; nohup ./start_gcp.sh > /root/scraper-logs/run.log 2>&1 &" \
	--zone=europe-west1-b

