"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import json
import logging
import math
from collections import Counter
from datetime import datetime, time, timedelta

import apache_beam as beam
import dateutil
import numpy as np
import pandas as pd
from apache_beam import DoFn, ParDo, WithKeys, GroupByKey
from apache_beam.io import fileio
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage


def aJson(stats, sample):
    return {
        "id": sample["exchangeId"],
        "runner_id": str(sample["runnerId"]),
        "ts": sample["ts"],
        #"delta": float("NaN"),
        "prediction": None,
        "back": round(sample["back"] * 100) / 100,
        "lay": round(sample["lay"] * 100) / 100,
        "start_lay": float("NaN"),
        "start_back": float("NaN"),
        "home": sample["home"],
        "hgoal": stats["home"]["goals"],
        "guest": sample["guest"],
        "agoal": stats["away"]["goals"],
        "runner_name": sample["runnerName"],
        "event_name": sample["eventName"],
        "event_id": sample["eventId"],
        "market_name": sample["marketName"],
        "market_id": sample["marketId"],
        "total_available": round(sample["totalAvailable"] * 100) / 100,
        "total_matched": round(sample["totalMatched"] * 100) / 100,
        "matched": round(sample["matched"] * 100) / 100,
        "available": round(sample["available"] * 100) / 100,
    }


def sample_and_goal_jsons(merged_tuple):
    data = merged_tuple[1]

    try:
        stats = data["stats"][0]
    except:
        print('AAAARHG')

    samples = data["samples"]
    output = []
    for sample in samples:
        if not stats["home"] or not stats["away"]:
            print("missing values for stats, event: " + sample["eventId"])
            continue

        output.append(aJson(stats, sample))

    return output

def hms_to_min(s):
    t = 0
    for u in s.strftime("%H:%M").split(':'):
        t = 60 * t + int(u)
    return t

def create_df_by_event(rows):
    rows.insert(0, ['id', 'runner_id', 'ts', 'prediction', 'back', 'lay', 'start_lay', 'start_back', 'hgoal',
                    'agoal', 'runner_name', 'event_name', 'event_id', 'market_name', 'available', 'matched',
                    'total_available', 'total_matched', 'draw_perc'])
    return pd.DataFrame(rows[1:], columns=rows[0])

def current_result_is (prediction, hgoal, agoal):
    if 'HOME' == prediction:
        if hgoal > agoal:
            return 'EXPECTED'
        elif hgoal == agoal:
            return 'DRAW'
        else:
            return 'WRONG'
    elif 'AWAY' == prediction:
        if hgoal < agoal:
            return 'EXPECTED'
        elif hgoal == agoal:
            return 'DRAW'
        else:
            return 'WRONG'

def drop_rule_out_goals(df):
    # use 121 because step is made by 120s see https://stackoverflow.com/questions/46105315/python-pandas-finding-derivatives-from-dataframe
    diff = df.set_index('ts').agoal.rolling('121s').apply(lambda x: x[-1] - x[0]) / 2
    diff = diff.reset_index(drop=True)

    negative_diff_indexes = diff[diff < 0]
    for index, value in negative_diff_indexes.items():
        real_agoals = list(df.iloc[[index]]['agoal'])[0]
        df.iloc[index - 1, df.columns.get_loc('agoal')] = real_agoals

    diff = df.set_index('ts').hgoal.rolling('121s').apply(lambda x: x[-1] - x[0]) / 2
    diff = diff.reset_index(drop=True)

    negative_diff_indexes = diff[diff < 0]
    for index, value in negative_diff_indexes.items():
        real_hgoals = list(df.iloc[[index]]['hgoal'])[0]
        df.iloc[index - 1, df.columns.get_loc('hgoal')] = real_hgoals

    return df

def interpolate_missing_ts (df):
    prediction = df.prediction.unique()[0]
    runner = df.runner_name.unique()[0]
    event_id = df.event_id.unique()[0]

    df['ts'] = df.apply(lambda x: dateutil.parser.isoparse(x.ts), axis=1)

    start = datetime.combine(df['ts'].min(), time.min)
    end = start + timedelta(minutes=120)

    idx = pd.date_range(start, end, freq='120S')
    df.set_index('ts', drop=True, inplace=True)
    df.index = pd.DatetimeIndex(df.index)
    df = df.reindex(idx, fill_value=None)
    df['ts'] = pd.DatetimeIndex(df.index)

    df['lay'] = df.apply(lambda row: float('NaN') if row.lay < 0 else row.lay, axis=1)

    df_interpol = df.resample('120S').mean()
    df_interpol['lay'] = df_interpol['lay'].interpolate()
    df_interpol['back'] = df_interpol['back'].interpolate()
    #df_interpol['delta'] = df_interpol['delta'].pad()
    df_interpol['hgoal'] = df_interpol['hgoal'].pad()
    df_interpol['agoal'] = df_interpol['agoal'].pad()
    df_interpol['start_back'] = df_interpol['start_back'].pad()
    df_interpol['start_lay'] = df_interpol['start_lay'].pad()
    df_interpol['available'] = df_interpol['available'].pad()
    df_interpol['matched'] = df_interpol['matched'].pad()
    df_interpol['total_matched'] = df_interpol['total_matched'].pad()
    df_interpol['total_available'] = df_interpol['total_available'].pad()
    df_interpol['draw_perc'] = df_interpol['draw_perc'].pad()

    #df_interpol = df_interpol.assign(event_id=lambda x: event)
    # once index is set, this assign statement create a column with the same length of the index and each row has the same value
    df_interpol = df_interpol.assign(prediction=lambda x: prediction)
    df_interpol = df_interpol.assign(event_id=lambda x: event_id)
    df_interpol = df_interpol.assign(runner_name=lambda x: runner)
    df_interpol = df_interpol.assign(lay=lambda row: round(row.lay, 2))

    # dlay = df_interpol.lay.rolling('121s').apply(lambda x: x[-1] - x[0]) / 2
    # df_interpol['dlay'] = dlay
    df_interpol['ts'] = df['ts']
    df_interpol['minute'] = df_interpol.apply(lambda row: hms_to_min(row.ts), axis=1)

    return df_interpol

def is_draw_match (df):

    results = np.array(df['current_result'])
    dict = Counter(results)
    draw = 0 if dict['DRAW'] is None else dict['DRAW']
    expected = 0 if dict['EXPECTED'] is None else dict['EXPECTED']
    wrong = 0 if dict['WRONG'] is None else dict['WRONG']
    other = expected + wrong
    p = (draw / results.size) * 100
    if p >= 85:
        logging.info('skip event because it is draw at ' + str(p) + "%, draw: " + str(draw) + " other: " + str(other))
        return True
    else:
        return False

def assign_goal_diff_by_prediction (df):
    df['goal_diff_by_prediction'] = df.apply(lambda row: row.hgoal - row.agoal if row.prediction == 'HOME' else row.agoal - row.hgoal, axis=1)
    return df


#TODO remove this column is useless
def assign_current_result(df):
    df['current_result'] = df.apply(lambda row: current_result_is(row.prediction, row.hgoal, row.agoal), axis=1)
    return df


class JsonReader(beam.PTransform):
    def expand(self, pcoll):
        return (
                pcoll
                # Bind window info to each element using element timestamp (or publish time).
                | "Read json from storage" >> ParDo(JsonParser())
        )


class JsonParser(DoFn):
    def process(self, file, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        # yield json.loads('{"id": "1", "market_name" : "test", "runner_name" : "test", "ts" : "2021-10-05T15:50:00.890Z", "lay": 1.0, "back" : 1.0}')
        if not file:
            logging.info("File read is null")
            yield json.loads('{}')

        data = file.decode('utf-8')
        if not data:
            logging.info("Json read is null")
            yield json.loads('{}')

        try:
            sample = json.loads(data)
        except BaseException as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise


        logging.info("Parsed json ")
        yield sample

class MatchRow (DoFn):
    def process(self, element):
        # event_id, competition_id, cutoff_date, delta, event_name, favourite, guest, home, score = element.split(";")
        return [{
            'event_id': element['event_id'],
            'competition_id': element['competition_id'],
            'cutoff_date': element['cutoff_date'],
            'event_name': element['event_name'],
            'guest': element['guest'],
            'home': element['home'],
            'favourite': element['favourite'],
            'score': element['outcome']
        }]

class RunnerRow (DoFn):
    def process(self, element):
        return [{
            'id': element['id'],
            'runner_id': element['runner_id'],
            'available': element['available'],
            'back': float(element['back']),
            'lay': float(element['lay']),
            'market_id': element['market_id'],
            'market_name': element['market_name'],
            'matched': element['matched'],
            'runner_name': element['runner_name'],
            'total_available': round(float(element['total_available']) * 100) / 100,
            'total_matched': round(float(element['total_matched']) * 100) / 100
        }]

class EnrichWithStartQuotes (DoFn):
    def process(self, tuple, runners):
        sample = tuple[1]
        runner_dict = {x['id'] + '_' + x['runner_id'] + '#' + x['market_name']: x for x in filter(lambda runner: runner['id'] == sample['event_id'], runners)}
        key = sample['id']

        if key in runner_dict.keys():
            if runner_dict[key]:
                runner = runner_dict[key]
                sample['start_lay'] = runner['lay']
                sample['start_back'] = runner['back']
        else:
            logging.warn("Missing " + key + " in runner table ")
            yield {}

        yield sample

class EnrichWithPrediction (DoFn):
    def process (self, tuple, matches):

        sample = tuple[1]
        match_dict = {x['event_id']: x for x in filter(lambda match: match['event_id'] == sample['event_id'], matches)}
        key = sample['event_id']

        if key in match_dict.keys():
            if match_dict[key]:
                match = match_dict[key]
                sample['prediction'] = match['favourite']
        else:
            logging.warn("Missing " + key + " in match table ")
            yield {}

        yield sample

class EnrichWithDrawPercentage (DoFn):
    def process(self, row_dict, scores):

        if not bool(row_dict):
            return {}

        if math.isnan(row_dict['start_back']):
            return {}

        start_back = row_dict['start_back']
        score_dict = list(filter(lambda score: start_back >= float(score['start']) and start_back <= float(score['end']), scores))[0]
        row_dict['draw_perc'] = score_dict['perc']
        yield row_dict

class ReadFileContent(beam.DoFn):

    # def setup(self):
    #     # Called whenever the DoFn instance is deserialized on the worker.
    #     # This means it can be called more than once per worker because multiple instances of a given DoFn subclass may be created (e.g., due to parallelization, or due to garbage collection after a period of disuse).
    #     # This is a good place to connect to database instances, open network connections or other resources.
    #     self.storage_client = storage.Client()

    def process(self, file_name, bucket):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket)
        blob = bucket.get_blob(file_name)
        yield blob.download_as_string()


def merge_df(dfs):
    if not dfs:
        logging.info("No dataframe to concat ")
        return pd.DataFrame([])

    return pd.concat(dfs).reset_index(drop=True)

def get_quote_and_score (merged_tuple):

    if not merged_tuple[1]['matches'] or not merged_tuple[1]['runners']:
        return {}

    match = merged_tuple[1]['matches'][0]
    runner = merged_tuple[1]['runners'][0]
    record = {}
    record['id'] = match['event_id']
    record['start_back'] = runner['back']
    record['score'] = match['score']
    return record

def select_start_back_interval(row):

    if not row:
        return str('-1')

    #extreme values
    min = 3.0
    max = 10.0

    if row['start_back'] < min:
        return str(0) + '-' + str(min)

    if row['start_back'] > max:
        return str(max) + '-' + str(row['start_back'])

    last_thr = -1
    for thr in np.arange(int(min), int(max), 0.50):
        if row['start_back'] >= thr and row['start_back'] < thr + 0.5:
            return str(thr) + '-' + str(thr+0.5)
        last_thr = thr

    return str(last_thr)

def calculate_draw_percentage(tuple):
    df = tuple[1]
    draw_match_num = df[(df['score'] == 'DRAW')].shape[0]
    total = df.shape[0]
    return {
        'start': tuple[0].split('-')[0],
        'end': tuple[0].split('-')[-1],
        'perc': round((draw_match_num / total) * 100) / 100
    }

def list_blobs(bucket, path):
    """Lists all the blobs in the bucket."""
    start_of_day = datetime.combine(datetime.utcnow(), time.min).strftime("%Y-%m-%d")
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket, prefix=start_of_day + '/' + path)
    json_paths = []
    for blob in blobs:
        #json_paths.append(f"gs://{bucket_name}/{blob.name}")
        json_paths.append(f"{blob.name}")
    return json_paths


def run(bucket, args=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        args, save_main_session=True
    )

    start_of_day = datetime.combine(datetime.utcnow(), time.min).strftime("%Y-%m-%d")
    with beam.Pipeline(options=pipeline_options) as pipeline:

        # match_table_spec = bigquery.TableReference(projectId='scraper-v1', datasetId='bet', tableId='match')
        # runner_table_spec = bigquery.TableReference(projectId='scraper-v1', datasetId='bet', tableId='runner')
        # match_dict = (
        #         pipeline
        #         # Each row is a dictionary where the keys are the BigQuery columns
        #         | 'Read match bq table' >> beam.io.ReadFromBigQuery(gcs_location='gs://dump-bucket-3/tmp/', table=match_table_spec)
        #         | "Convert list in row " >> ParDo(MatchRow())
        #         | "Filter matches without favourite" >> beam.Filter(lambda row: row['favourite'] is not None)
        # )
        #
        # runner_dict = (
        #         pipeline
        #         # Each row is a dictionary where the keys are the BigQuery columns
        #         | 'Read runner bq table' >> beam.io.ReadFromBigQuery(gcs_location='gs://dump-bucket-3/tmp/', table=runner_table_spec)
        #         | "Parse runner row " >> beam.ParDo(RunnerRow())
        # )
        #
        # match_dict_with_key = (match_dict | "add key for match" >> WithKeys(lambda x: x['event_id']))
        # runner_dict_with_key = (runner_dict | "add key for runner " >> WithKeys(lambda x: x['id']))
        #
        # draw_percentage_by_start_back_interval = (
        #         ({'matches': match_dict_with_key, 'runners': runner_dict_with_key})
        #         | 'Join match and runners' >> beam.CoGroupByKey()
        #         | 'get start back and score' >> beam.Map(lambda x: get_quote_and_score(x))
        #         | 'Use start_back interval as key ' >> WithKeys(lambda row: select_start_back_interval(row))
        #         | 'Drop invalid keys  ' >> beam.Filter(lambda tuple: tuple[0] != '-1')
        #         | 'group by start back interval ' >> GroupByKey()
        #         | 'create score df ' >> beam.Map(lambda tuple: (tuple[0], pd.DataFrame(tuple[1])))
        #         | 'calculate draw percentage ' >> beam.Map(lambda tuple: calculate_draw_percentage(tuple))
        # )

        samples_tuple = (
                pipeline
                | 'Create' >> beam.Create(list_blobs(bucket, start_of_day + '/live'))
                | 'Read each file content' >> beam.ParDo(ReadFileContent(), bucket)
                | "Convert sample file to json" >> ParDo(JsonParser())
                #| "Flatten samples " >> beam.FlatMap(lambda x: x)
                #| "map samples " >> beam.Map(lambda x: x)
                | "Add key to samples " >> WithKeys(lambda x: x['eventId'] + '#' + x['ts'])
        )

        # stats_tuple = (
        #         pipeline
        #         | "Matching stats" >> fileio.MatchFiles('gs://' + bucket + '/' + start_of_day.strftime('%Y-%m-%dT%H:%M:%S.000Z') + '/dump/stats/*.json')
        #         | "Reading stats " >> fileio.ReadMatches()
        #         | "Convert stats file to json" >> JsonReader()
        #         | "Flatten stats " >> beam.FlatMap(lambda x: x)
        #         | "map stats " >> beam.Map(lambda x: x)
        #         | "Add key to stats " >> WithKeys(lambda x: x['eventId'] + '#' + x['ts'])
        # )
        #
        # sample_with_score_tuples = (
        #         ({'samples': samples_tuple, 'stats': stats_tuple})
        #         | 'Merge back record' >> beam.CoGroupByKey()
        #         | 'remove empty stats ' >> beam.Filter(lambda merged_tuple: len(merged_tuple[1]['samples']) > 0 and len(merged_tuple[1]['stats']) > 0)
        #         | 'Getting back record' >> beam.FlatMap(lambda x: sample_and_goal_jsons(x))
        #         | "add key " >> WithKeys(lambda x: x['event_id'] + '#' + x['runner_id'])
        # )
        #
        # samples_enriched_with_start_quotes = (
        #         sample_with_score_tuples
        #         | 'Enrich sample with start quotes' >> beam.ParDo(EnrichWithStartQuotes(), beam.pvalue.AsList(runner_dict))
        #         | 'Remove empty sample for missing runner ' >> beam.Filter(lambda sample: bool(sample))
        #         | "Add key to join between pre/live/scores " >> WithKeys(lambda merged_json: merged_json['event_id'])
        # )
        #
        # out_csv = 'gs://' + bucket + '/stage/data_' + start_of_day.strftime('%Y-%m-%d') + '.csv'
        # _ = (samples_enriched_with_start_quotes
        #         | 'Enrich sample with home and guest ' >> beam.ParDo(EnrichWithPrediction(), beam.pvalue.AsList(match_dict))
        #         | 'Enrich sample with draw percentage ' >> beam.ParDo(EnrichWithDrawPercentage(), beam.pvalue.AsList(draw_percentage_by_start_back_interval))
        #         | 'Remove empty sample for missing match ' >> beam.Filter(lambda sample: bool(sample))
        #         | 'add event_id as key' >> WithKeys(lambda row : row['event_id'])
        #         | 'group by key' >> GroupByKey()
        #         | 'get list of rows ' >> beam.Map(lambda tuple : tuple[1])
        #         | 'create dataframe for an event ' >> beam.Map(lambda rows: create_df_by_event(rows))
        #         | 'remove duplicated ts' >> beam.Map(lambda df: df.drop_duplicates(subset='ts', keep='first'))
        #         | 'interpolate quote values for missing ts ' >> beam.Map(lambda df: interpolate_missing_ts(df))
        #         | 'drop rule out goals ' >> beam.Map(lambda df: drop_rule_out_goals(df))
        #         | 'add sum_goal column ' >> beam.Map(lambda df: df.assign(sum_goals=lambda row: row.agoal + row.hgoal))
        #         | 'add current_result column' >> beam.Map(lambda df: assign_current_result(df))
        #         | 'add goal_diff_by_prediction column' >> beam.Map(lambda df: assign_goal_diff_by_prediction(df))
        #         | 'drop draw matches ' >> beam.Filter(lambda df: not is_draw_match(df))
        #         | 'merge all dataframe ' >> beam.CombineGlobally(lambda dfs: merge_df(dfs))
        #         | 'filter empty dataframe ' >> beam.Filter(lambda df: not df.empty)
        #         | 'write to csv ' >> beam.Map(lambda df: df.to_csv(out_csv, sep=';', index=False, encoding="utf-8", line_terminator='\n'))
        #     )

    logging.info("pipeline started")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--bucket',
        dest='bucket',
        required=True,
        help='bucket where read/write csv'
    )

    known_args, pipeline_args = parser.parse_known_args()
    run(known_args.bucket, pipeline_args)