#!/bin/bash

apt update
apt install aptitude -y
aptitude install nodejs -y
aptitude install npm -y

cd root
mkdir exchange
cd exchange

git init
git remote add origin https://AxlRoe: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@github.com/AxlRoe/exchange.git
git pull origin master

npm install
#npm start


