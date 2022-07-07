#!/bin/bash

sudo apt update
sudo apt install aptitude -y
sudo aptitude install maven -y
sudo aptitude install openjdk-11-jre-headless -y

cd /home/ubuntu
mkdir scraper-logs
chown -R ubuntu:ubuntu scraper-logs
mkdir bet-scraper-orchestrator
chown -R ubuntu:ubuntu bet-scraper-orchestrator
cd bet-scraper-orchestrator
git init
git remote add origin https://AxlRoe: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@github.com/AxlRoe/bet-scraper-orchestrator
git pull origin master
git pull
git checkout -b cloud_redis_test origin/cloud_redis_test

mvn clean install
cd src/main/compute
chmod +x start.sh
./start.sh

