"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import ast
import json
import logging
import math
import random
from collections import Counter
from datetime import datetime, time, timedelta

import apache_beam as beam
import dateutil
import numpy as np
import pandas as pd
from apache_beam import DoFn, ParDo, WithKeys, GroupByKey
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window

SCHEMA = ",".join(
    [
        "id:STRING",
        "market_name:STRING",
        "runner_name:STRING",
        "lay:FLOAT64",
        "back:FLOAT64",
        "ts: TIMESTAMP"
    ]
)

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

class CSVParser(DoFn):
    def process (self, file):
        data = file.read_utf8()
        lines = data.split("\n")
        yield lines[1:-1]


class JsonParser(DoFn):
    def process(self, file, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        # yield json.loads('{"id": "1", "market_name" : "test", "runner_name" : "test", "ts" : "2021-10-05T15:50:00.890Z", "lay": 1.0, "back" : 1.0}')
        data = file.read_utf8()
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


class RecordToGCSBucket(beam.PTransform):

    def __init__(self, num_shards=5):
        # Set window size to 60 seconds.
        self.num_shards = num_shards

    def expand(self, pcoll):

        def gcs_path_builder(message):
            k, record = message
            # the records have 'value' attribute when --with_metadata is given
            if hasattr(record, 'value'):
                message_bytes = record.value
            elif isinstance(record, tuple):
                message_bytes = record[1]
            elif isinstance(record, list):
                message_bytes = record[0]
            else:
                raise RuntimeError('unknown record type: %s' % type(record))
            # Converting bytes record from Kafka to a dictionary.
            message = ast.literal_eval(message_bytes.decode("UTF-8"))
            logging.info("MSG IS " + str(message))
            return 'gs://data-flow-bucket_1/' + message['event_id'] + '/*.json'
            # return 'C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\' + message['event_id'] + '\\*.json'

        return (
                pcoll
                # Bind window info to each element using element timestamp (or publish time).
                | "Window into fixed intervals" >> beam.WindowInto(window.FixedWindows(15, 0))
                | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
                # Group windowed elements by key. All the elements in the same window must fit
                # memory for this. If not, you need to use `beam.util.BatchElements`.
                | "Group by key" >> GroupByKey()
                | "Read event id from message" >> beam.Map(lambda message: gcs_path_builder(message))
                | "Read files to ingest " >> fileio.MatchAll()
                | "Convert result from match file to readable file " >> fileio.ReadMatches()
                | "shuffle " >> beam.Reshuffle()
                | "Convert file to json" >> JsonReader()
                | "Flatten samples " >> beam.FlatMap(lambda x: x)
        )

class MatchRow (DoFn):
    def process(self, element):
        event_id, competition_id, cutoff_date, delta, event_name, favourite, guest, home, score = element.split(";")
        if favourite == '':
            return []

        return [{
            'event_id': event_id,
            'competition_id': competition_id,
            'cutoff_date': cutoff_date,
            'event_name': event_name,
            'guest': guest,
            'home': home,
            'favourite': favourite,
            'score': score
        }]

class RunnerRow (DoFn):
    def process(self, element):
        id, market_id, runner_id, available, back, lay, market_name, matched, runner_name, total_available, total_matched = element.split(";")
        return [{
            'id': id,
            'runner_id': runner_id,
            'available': available,
            'back': float(back),
            'lay': float(lay),
            'market_id': market_id,
            'market_name': market_name,
            'matched': matched,
            'runner_name': runner_name,
            'total_available': round(float(total_available) * 100) / 100,
            'total_matched': round(float(total_matched) * 100) / 100
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

#TODO non viene calcolato su tutti i match ma solo su quelli giorno per giorno
def calculate_draw_percentage(tuple):
    df = tuple[1]
    draw_match_num = df[(df['score'] == 'DRAW')].shape[0]
    total = df.shape[0]
    return {
        'start': tuple[0].split('-')[0],
        'end': tuple[0].split('-')[-1],
        'perc': round((draw_match_num / total) * 100) / 100
    }


def run(bootstrap_servers, match_csv, runner_csv, out_csv, args=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        args, streaming=True, save_main_session=True
    )

    # with Pipeline(options=pipeline_options) as pipeline:
    #     (pipeline
    #      # | ReadFromKafka(consumer_config={'bootstrap.servers': bootstrap_servers},
    #      #                 topics=['exchange.ended.events'])
    #      | "Read from Pub/Sub" >> ReadFromPubSub(topic='projects/data-flow-test-327119/topics/exchange.ended.events').with_output_types(bytes)
    #      | "Read files " >> RecordToGCSBucket(5)
    #      | "Write to BigQuery" >> bigquery.WriteToBigQuery(bigquery.TableReference(
    #                 projectId='data-flow-test-327119',
    #                 datasetId='kafka_to_bigquery',
    #                 tableId='transactions'),
    #                 schema=SCHEMA,
    #                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    #                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    #      )

    with beam.Pipeline(options=pipeline_options) as pipeline:

        match_dict = (
                pipeline
                | "Matching csvs" >> fileio.MatchFiles('C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\csv_enrich\\*.csv')
                | "Reading csvs" >> fileio.ReadMatches()
                | "Conver csvs in list of lists" >> ParDo(CSVParser())
                | "flatten " >> beam.FlatMap(lambda x: x)
                | "Convert list in row " >> ParDo(MatchRow())
                | "Filter matches without favourite" >> beam.Filter(lambda row: row['favourite'] is not None)
        )

        runner_dict = (
                pipeline
                | "Matching runner csvs" >> fileio.MatchFiles('C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\csv\\runner*.csv')
                | "Reading runner csvs" >> fileio.ReadMatches()
                | "Convert runner csvs in list of lists" >> ParDo(CSVParser())
                | "flatten runner lines" >> beam.FlatMap(lambda x: x)
                | "Parse runner row " >> beam.ParDo(RunnerRow())
        )

        match_dict_with_key = (match_dict | "add key for match" >> WithKeys(lambda x: x['event_id']))
        runner_dict_with_key = (runner_dict | "add key for runner " >> WithKeys(lambda x: x['id']))

        draw_percentage_by_start_back_interval = (
                ({'matches': match_dict_with_key, 'runners': runner_dict_with_key})
                | 'Join match and runners' >> beam.CoGroupByKey()
                | 'get start back and score' >> beam.Map(lambda x: get_quote_and_score(x))
                | 'Use start_back interval as key ' >> WithKeys(lambda row: select_start_back_interval(row))
                | 'Drop invalid keys  ' >> beam.Filter(lambda tuple: tuple[0] != '-1')
                | 'group by start back interval ' >> GroupByKey()
                | 'create score df ' >> beam.Map(lambda tuple: (tuple[0], pd.DataFrame(tuple[1])))
                | 'calculate draw percentage ' >> beam.Map(lambda tuple: calculate_draw_percentage(tuple))
        )

        samples_tuple = (
                pipeline
                | "Matching samples" >> fileio.MatchFiles('C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\samples\\*.json')
                | "Reading sampling" >> fileio.ReadMatches()
                | "Convert sample file to json" >> JsonReader()
                | "Flatten samples " >> beam.FlatMap(lambda x: x)
                | "map samples " >> beam.Map(lambda x: x)
                | "Add key to samples " >> WithKeys(lambda x: x['eventId'] + '#' + x['ts'])
        )

        stats_tuple = (
                pipeline
                | "Matching stats" >> fileio.MatchFiles('C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\stats\\*.json')
                | "Reading stats " >> fileio.ReadMatches()
                | "Convert stats file to json" >> JsonReader()
                | "Flatten stats " >> beam.FlatMap(lambda x: x)
                | "map stats " >> beam.Map(lambda x: x)
                | "Add key to stats " >> WithKeys(lambda x: x['eventId'] + '#' + x['ts'])
        )

        sample_with_score_tuples = (
                ({'samples': samples_tuple, 'stats': stats_tuple})
                | 'Merge back record' >> beam.CoGroupByKey()
                | 'remove empty stats ' >> beam.Filter(lambda merged_tuple: len(merged_tuple[1]['samples']) > 0 and len(merged_tuple[1]['stats']) > 0)
                | 'Getting back record' >> beam.FlatMap(lambda x: sample_and_goal_jsons(x))
                | "add key " >> WithKeys(lambda x: x['event_id'] + '#' + x['runner_id'])
        )

        samples_enriched_with_start_quotes = (
                sample_with_score_tuples
                | 'Enrich sample with start quotes' >> beam.ParDo(EnrichWithStartQuotes(), beam.pvalue.AsList(runner_dict))
                | 'Remove empty sample for missing runner ' >> beam.Filter(lambda sample: bool(sample))
                | "Add key to join between pre/live/scores " >> WithKeys(lambda merged_json: merged_json['event_id'])
        )

        _ = (samples_enriched_with_start_quotes
                | 'Enrich sample with home and guest ' >> beam.ParDo(EnrichWithPrediction(), beam.pvalue.AsList(match_dict))
                | 'Enrich sample with draw percentage ' >> beam.ParDo(EnrichWithDrawPercentage(), beam.pvalue.AsList(draw_percentage_by_start_back_interval))
                | 'Remove empty sample for missing match ' >> beam.Filter(lambda sample: bool(sample))
                | 'add event_id as key' >> WithKeys(lambda row : row['event_id'])
                | 'group by key' >> GroupByKey()
                | 'get list of rows ' >> beam.Map(lambda tuple : tuple[1])
                | 'create dataframe for an event ' >> beam.Map(lambda rows: create_df_by_event(rows))
                | 'remove duplicated ts' >> beam.Map(lambda df: df.drop_duplicates(subset='ts', keep='first'))
                | 'interpolate quote values for missing ts ' >> beam.Map(lambda df: interpolate_missing_ts(df))
                | 'drop rule out goals ' >> beam.Map(lambda df: drop_rule_out_goals(df))
                | 'add sum_goal column ' >> beam.Map(lambda df: df.assign(sum_goals=lambda row: row.agoal + row.hgoal))
                | 'add current_result column' >> beam.Map(lambda df: assign_current_result(df))
                | 'add goal_diff_by_prediction column' >> beam.Map(lambda df: assign_goal_diff_by_prediction(df))
                | 'drop draw matches ' >> beam.Filter(lambda df: not is_draw_match(df))
                | 'merge all dataframe ' >> beam.CombineGlobally(lambda dfs: merge_df(dfs))
                | 'filter empty dataframe ' >> beam.Filter(lambda df: not df.empty)
                | 'write to csv ' >> beam.Map(lambda df: df.to_csv(out_csv, sep=';', index=False, encoding="utf-8", line_terminator='\n'))
            )

    logging.info("pipeline started")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bootstrap_servers',
        dest='bootstrap_servers',
        required=True,
        help='Bootstrap servers for the Kafka cluster. Should be accessible by the runner'
    )
    parser.add_argument(
        '--match_csv',
        dest='match_csv',
        required=True,
        help='csv with the match to consider'
    )
    parser.add_argument(
        '--runner_csv',
        dest='runner_csv',
        required=True,
        help='csv with the runner to consider'
    )

    parser.add_argument(
        '--out_csv',
        dest='out_csv',
        required=True,
        help='csv with the output of the pipeline'
    )

    known_args, pipeline_args = parser.parse_known_args()
    run(known_args.bootstrap_servers, known_args.match_csv, known_args.runner_csv, known_args.out_csv, pipeline_args)