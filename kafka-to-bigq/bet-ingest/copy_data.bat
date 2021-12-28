del /f data.csv
rmdir /S /q prematch stats samples
mkdir stats samples prematch

copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_%1\recover\live_* samples
copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_%1\recover\0m\prematch_* prematch
copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_%1\recover\stats\stats_* stats

rem virtualenv env
rem env\Scripts\activate.exe
rem pip install -U -r requirements.txt

python pipeline.py --bootstrap_servers localhost:9092

copy data.csv C:\Users\mmarini\MyGit\py-bet-analyzer\ml\csv\bet_%1.csv

