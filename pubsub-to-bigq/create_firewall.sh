#!/bin/bash

gcloud compute firewall-rules create default-allow-scraping --source-ranges '0.0.0.0/0' --action allow --target-tags etl --rules tcp:3000 

