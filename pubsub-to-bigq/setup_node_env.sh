#!/bin/bash

apt update
apt install aptitude -y
aptitude install nodejs -y
aptitude install npm -y

cd /home/james_marxista
mkdir bet-ui
cd bet-ui

git init
git remote add origin https://AxlRoe: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@github.com/AxlRoe/bet-ui.git
git pull origin master

npm install
npm start


