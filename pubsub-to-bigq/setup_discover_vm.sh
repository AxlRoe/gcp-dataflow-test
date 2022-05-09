#!/bin/bash

sudo apt update
sudo apt install aptitude -y
sudo aptitude install maven -y
sudo aptitude install openjdk-11-jre-headless -y

cd /home/james_marxista
mkdir scraper-logs
chown -R james_marxista:james_marxista scraper-logs
mkdir bet-discover
chown -R james_marxista:james_marxista bet-discover
cd bet-discover
git init
git remote add origin https://AxlRoe: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@github.com/AxlRoe/bet-discover
git pull origin master
git pull
git checkout -b cloud_redis_test origin/cloud_redis_test

mvn clean install
cd src/main/compute
chmod +x start.sh
./start.sh

