#!/bin/bash

 apt update
 apt install aptitude -y
 aptitude install python3-pip -y
 aptitude install python3-virtualenv -y
 aptitude install build-essential libssl-dev libffi-dev python3-dev -y
 aptitude install libpq-dev -y

cd /home/james_marxista
mkdir bet-odds-monitor
cd bet-odds-monitor

git init
git remote add origin https://AxlRoe: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@github.com/AxlRoe/bet-odds-monitor
git pull origin master
git pull
git checkout -b cloud_redis_test origin/cloud_redis_test

virtualenv venv
source venv/bin/./activate

pip install -r requirements.txt

chmod +x compute/./start_gcp.sh
compute/./start_gcp.sh

