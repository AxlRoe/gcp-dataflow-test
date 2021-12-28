del /f data.csv
rmdir /S /q prematch stats samples
mkdir stats samples prematch

copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_18_12\recover\live_* samples
copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_18_12\recover\0m\prematch_* prematch
copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_18_12\recover\stats\stats_* stats

rem virtualenv env
rem env\Scripts\activate.exe
rem pip install -U -r requirements.txt

python pipeline.py --bootstrap_servers localhost:9092

copy data.csv C:\Users\mmarini\MyGit\py-bet-analyzer\ml\csv\bet_18_12.csv

del /f data.csv
rmdir /S /q prematch stats samples
mkdir stats samples prematch

copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_19_12\recover\live_* samples
copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_19_12\recover\0m\prematch_* prematch
copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_19_12\recover\stats\stats_* stats

rem virtualenv env
rem env\Scripts\activate.exe
rem pip install -U -r requirements.txt

python pipeline.py --bootstrap_servers localhost:9092

copy data.csv C:\Users\mmarini\MyGit\py-bet-analyzer\ml\csv\bet_19_12.csv

del /f data.csv
rmdir /S /q prematch stats samples
mkdir stats samples prematch

copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_21_12\recover\live_* samples
copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_21_12\recover\0m\prematch_* prematch
copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_21_12\recover\stats\stats_* stats

rem virtualenv env
rem env\Scripts\activate.exe
rem pip install -U -r requirements.txt

python pipeline.py --bootstrap_servers localhost:9092

copy data.csv C:\Users\mmarini\MyGit\py-bet-analyzer\ml\csv\bet_21_12.csv

del /f data.csv
rmdir /S /q prematch stats samples
mkdir stats samples prematch

copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_22_12\recover\live_* samples
copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_22_12\recover\0m\prematch_* prematch
copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_22_12\recover\stats\stats_* stats

rem virtualenv env
rem env\Scripts\activate.exe
rem pip install -U -r requirements.txt

python pipeline.py --bootstrap_servers localhost:9092

copy data.csv C:\Users\mmarini\MyGit\py-bet-analyzer\ml\csv\bet_22_12.csv


del /f data.csv
rmdir /S /q prematch stats samples
mkdir stats samples prematch

copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_23_12\recover\live_* samples
copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_23_12\recover\0m\prematch_* prematch
copy C:\Users\mmarini\MyGit\betanalyzer\exchange\dump_23_12\recover\stats\stats_* stats

rem virtualenv env
rem env\Scripts\activate.exe
rem pip install -U -r requirements.txt

python pipeline.py --bootstrap_servers localhost:9092

copy data.csv C:\Users\mmarini\MyGit\py-bet-analyzer\ml\csv\bet_23_12.csv

