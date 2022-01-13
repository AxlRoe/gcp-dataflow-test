@echo off
del /f data.csv
rmdir /S /q prematch stats samples
mkdir stats samples prematch

cd C:\Users\mmarini\MyGit\betanalyzer\exchange
ren dump_%1 dump

cd dump
rmdir /S /q recover

mkdir recover
mkdir recover\stats
mkdir recover\12h
mkdir recover\6h
mkdir recover\3h
mkdir recover\1h
mkdir recover\30m
mkdir recover\5m
mkdir recover\10m
mkdir recover\2m
mkdir recover\1m
mkdir recover\0m

psql -c "delete from match where 1=1" -U postgres postgres
psql -c "delete from task_execution where 1=1" -U postgres postgres
psql -c "delete from runner where 1=1" -U postgres postgres

rem psql -U postgres postgres
psql -c "\copy match FROM 'C:\Users\mmarini\MyGit\bet-scheduler\data_"%1"\match.csv' delimiter ';' csv HEADER" -U postgres postgres
psql -c "\copy task_execution FROM 'C:\Users\mmarini\MyGit\bet-scheduler\data_"%1"\task_execution.csv' delimiter ';' csv HEADER" -U postgres postgres

rem curl --location --request POST "http://localhost:9000/scraper/recover" --data-raw "{""to"" : ""2021-12-26T23:00:00"", ""since"" : ""2021-12-26T00:00:00""}" -H "Content-Type: application/json"
curl --location --request POST "http://localhost:9000/scraper/recover" --data-raw "{""to"" : ""%2"", ""since"" : ""%3""}" -H "Content-Type: application/json"

copy recover\live_* C:\Users\mmarini\MyGit\gcp-dataflow-test\kafka-to-bigq\bet-ingest\samples
copy recover\0m\prematch_* C:\Users\mmarini\MyGit\gcp-dataflow-test\kafka-to-bigq\bet-ingest\prematch
copy recover\stats\stats_* C:\Users\mmarini\MyGit\gcp-dataflow-test\kafka-to-bigq\bet-ingest\stats

cd ..
ren dump dump_%1

cd C:\Users\mmarini\MyGit\gcp-dataflow-test\kafka-to-bigq\bet-ingest

python pipeline.py --bootstrap_servers localhost:9092

copy data.csv C:\Users\mmarini\MyGit\py-bet-analyzer\ml\csv\bet_%1.csv

