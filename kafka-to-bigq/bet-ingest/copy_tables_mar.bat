@echo off

echo "clean database"
psql -c "delete from match where 1=1" -U postgres postgres
psql -c "delete from runner where 1=1" -U postgres postgres
psql -c "delete from task_execution where 1=1" -U postgres postgres

echo "fill database with match and runners"
psql -c "\copy match FROM 'C:\Users\mmarini\MyGit\bet-data-pump\data_"%1"\match.csv' delimiter ';' csv HEADER" -U postgres postgres
psql -c "\copy runner FROM 'C:\Users\mmarini\MyGit\bet-data-pump\data_"%1"\runner.csv' delimiter ';' csv HEADER" -U postgres postgres
psql -c "\copy task_execution FROM 'C:\Users\mmarini\MyGit\bet-data-pump\data_"%1"\task_execution.csv' delimiter ';' csv HEADER" -U postgres postgres

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

rem rem curl --location --request POST "http://localhost:9000/scraper/recover" --data-raw "{""to"" : ""2021-12-27T23:00:00"", ""since"" : ""2021-12-27T00:00:00""}" -H "Content-Type: application/json"
curl --location --request POST "http://localhost:9000/scraper/recover" --data-raw "{""to"" : ""%2"", ""since"" : ""%3""}" -H "Content-Type: application/json"

copy recover\0m\prematch_* C:\Users\mmarini\MyGit\gcp-dataflow-test\kafka-to-bigq\bet-ingest\prematch
copy recover\live_* C:\Users\mmarini\MyGit\gcp-dataflow-test\kafka-to-bigq\bet-ingest\samples
copy recover\stats\stats_* C:\Users\mmarini\MyGit\gcp-dataflow-test\kafka-to-bigq\bet-ingest\stats

cd ..
ren dump dump_%1

cd C:\Users\mmarini\MyGit\gcp-dataflow-test\kafka-to-bigq\bet-ingest
psql -c "\copy (select * from match where cutoff_date < TO_DATE('"%4"','YYYYMMDD')) To 'csv\match_"%1".csv' delimiter ';' csv HEADER ENCODING 'UTF8'" -U postgres postgres
psql -c "\copy (select * from runner) To 'csv\runner_"%1".csv' delimiter ';' csv HEADER ENCODING 'UTF8'" -U postgres postgres

python match_score_pipeline.py --match_csv match_%1.csv --out_csv match_%1.csv
