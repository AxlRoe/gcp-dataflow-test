"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import json
import logging
import math
from datetime import datetime, time

import apache_beam as beam
import jsonpickle
import numpy as np
import pandas as pd
import sqlalchemy
from apache_beam import DoFn, ParDo, WithKeys, GroupByKey
from apache_beam.io import WriteToText, ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud.sql.connector import Connector, IPTypes


class JsonParser(DoFn):
    def process(self, file, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        if not file:
            logging.info("File read is null")
            yield json.loads('{}')

        try:
            samples = jsonpickle.decode(file)
            if not samples:
                logging.info("Json read is null")
                yield json.loads('{}')

            #logging.info("Parsed json ")
            yield samples

        except BaseException as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise

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
            'back': float(element['back']),
            'lay': float(element['lay'])
        }]

class EnrichWithStartQuotes (DoFn):
    def process(self, sample, runners):

        runner_dict = {x['id']: x for x in filter(lambda runner: runner['id'] == sample['event_id'], runners)}

        if not sample['event_id'] in runner_dict.keys():
            logging.warn("Missing " + str(sample['event_id']) + " in runner table ")
            yield {}

        runner = runner_dict[sample['event_id']]
        sample['start_lay'] = runner['lay']
        sample['start_back'] = runner['back']

        yield sample

class EnrichWithPrediction (DoFn):
    def process (self, tuple, matches):

        sample = tuple[1]
        match_dict = {x['event_id']: x for x in filter(lambda match: match['event_id'] == sample['event_id'], matches)}

        if not sample['event_id'] in match_dict.keys():
            logging.warn("Missing " + str(sample['event_id']) + " in match table ")
            yield {}

        match = match_dict[sample['event_id']]
        sample['prediction'] = match['favourite']

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


class ReadFromDBFn(beam.DoFn):

    def __init__(self, url, query, query_params={}, *args, **kwargs):
        super(ReadFromDBFn, self).__init__(*args, **kwargs)
        self.url = url
        self.query = query
        self.query_params = query_params

    def process(self, data, **kwargs):
        data = dict(data)

        def getconn():
            with Connector() as connector:
                conn = connector.connect(
                    "scraper-vx:europe-west1:scraper-db-3",
                    "pg8000",
                    user="postgres",
                    password="postgres",
                    db="postgres",
                    ip_type=IPTypes.PUBLIC,
                    enable_iam_auth=True
                )
            return conn

        pool = sqlalchemy.create_engine('postgresql+pg8000://', creator=getconn)

        with pool.connect() as db_conn:

            query_params = self.query_params

            if 'db_query_params' in data:
                query_params = data['db_query_params']

            for record in db_conn.execute(sqlalchemy.sql.text(self.query), query_params):
                yield dict(record)


def run(db_url, args=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        args, save_main_session=True
    )

    def aJson(sample):

        if not sample["homeStats"] or not sample["awayStats"]:
            return {}

        return {
            "event_id": sample["eventId"],
            "runner_name": sample["runnerName"],
            "minute": int(sample["minute"]),
            "prediction": None,
            "back": round(sample["back"] * 100) / 100,
            "lay": round(sample["lay"] * 100) / 100,
            "start_lay": float("NaN"),
            "start_back": float("NaN"),
            "home": sample["home"],
            "hgoal": sample["homeStats"]["goals"],
            "guest": sample["guest"],
            "agoal": sample["awayStats"]["goals"],
            "score": sample["score"]
        }

    def calculate_draw_percentage(tuple):
        df = tuple[1]
        draw_match_num = df[(df['score'] == 'DRAW')].shape[0]
        total = df.shape[0]
        return {
            'start': tuple[0].split('-')[0],
            'end': tuple[0].split('-')[-1],
            'perc': round((draw_match_num / total) * 100) / 100
        }

    def get_quote_and_score(merged_tuple):

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

        # extreme values
        min = 3.0
        max = 10.0

        if row['start_back'] < min:
            return str(0) + '-' + str(min)

        if row['start_back'] > max:
            return str(max) + '-' + str(row['start_back'])

        last_thr = -1
        for thr in np.arange(int(min), int(max), 0.50):
            if row['start_back'] >= thr and row['start_back'] < thr + 0.5:
                return str(thr) + '-' + str(thr + 0.5)
            last_thr = thr

        return str(last_thr)

    def create_df_by_event(rows):
        rows.insert(0, ['event_id','runner_name','minute','prediction','back','lay','start_lay','start_back','hgoal','agoal','draw_perc','score'])
        return  pd.DataFrame(rows[1:], columns=rows[0])

    def drop_rule_out_goals(df):
        df = df.sort_values('minute')
        def assign_real_goal_value(df, goal_col, goal_diff):
            df[goal_diff] = df[goal_col].diff()

            tmp_df = df[df[goal_diff] > 0]
            if not tmp_df.empty:
                goal_minutes = list(tmp_df['minute'])

            tmp_df = df[df[goal_diff] < 0]
            rule_out_goal_minutes = []
            if not tmp_df.empty:
                rule_out_goal_minutes = list(tmp_df['minute'])

            if not rule_out_goal_minutes:
                return df

            df = df.set_index('minute')
            for rg_min in rule_out_goal_minutes:
                start_rg_min = list(filter(lambda minute: minute < rg_min, goal_minutes))[0]
                real_agoals = df.loc[rg_min][goal_col]
                df.loc[list(range(start_rg_min, rg_min, 2)), goal_col] = real_agoals

            df = df.reset_index()
            df = df.drop(columns=[goal_diff])
            return df

        df = assign_real_goal_value(df, 'agoal', 'ag_diff')
        df = assign_real_goal_value(df, 'hgoal', 'hg_diff')

        return df

    def interpolate_missing_ts(df):

        idx = list(range(0, 120+2, 2))
        tmp_df = df.set_index('minute').reindex(idx).reset_index()

        tmp_df['lay'] = tmp_df.apply(lambda row: float('NaN') if row.lay < 0 else row.lay, axis=1)

        tmp_df['lay'] = tmp_df['lay'].pad()
        tmp_df['back'] = tmp_df['back'].pad()
        tmp_df['hgoal'] = tmp_df['hgoal'].pad()
        tmp_df['agoal'] = tmp_df['agoal'].pad()
        tmp_df['start_back'] = tmp_df['start_back'].pad()
        tmp_df['start_lay'] = tmp_df['start_lay'].pad()
        tmp_df['draw_perc'] = tmp_df['draw_perc'].pad()
        tmp_df['prediction'] = tmp_df['prediction'].pad()
        tmp_df['runner_name'] = tmp_df['runner_name'].pad()
        tmp_df['event_id'] = tmp_df['event_id'].pad()
        tmp_df['score'] = tmp_df['score'].pad()

        return tmp_df

    def is_draw_match(df):
        tmp_df = df[['agoal','hgoal']]
        tmp_df['sum_goal'] = tmp_df.apply(lambda row: row.agoal + row.hgoal, axis=1)
        tmp_df['draw'] = tmp_df['sum_goal'].where(tmp_df['sum_goal'] % 2 == 0)
        total = tmp_df['sum_goal'].size
        draw = tmp_df[~tmp_df['draw'].isna()]['draw'].size #get rows with draw score
        p = (draw / total) * 100
        if p >= 85:
            logging.info('skip event because it is draw at ' + str(p) + "%")
            return True
        else:
            return False

    def assign_goal_diff_by_prediction(df):
        df['goal_diff_by_prediction'] = df.apply(lambda row: row.hgoal - row.agoal if row.prediction == 'HOME' else row.agoal - row.hgoal, axis=1)
        return df

    def merge_df(dfs):
        if not dfs:
            logging.info("No dataframe to concat ")
            return pd.DataFrame([])

        return pd.concat(dfs).reset_index(drop=True)

    start_of_day = datetime.combine(datetime.utcnow(), time.min)
    start_of_day = '2022-08-11' #start_of_day.strftime("%Y-%m-%d") #'2022-06-27'
    bucket = 'dump-bucket-4'
    query_match = 'select * from match'
    query_runner = 'select * from runner'

    with beam.Pipeline(options=pipeline_options) as pipeline:

        match_dict = (
                pipeline
                # Each row is a dictionary where the keys are the BigQuery columns
                | "Initialize match extraction" >> beam.Create([{}]) #needed otherwise get Pbeing windowing error
                | 'Read match table' >> beam.ParDo(ReadFromDBFn(url=db_url, query=query_match))
                | "Convert list in row " >> ParDo(MatchRow())
                | "Filter matches without favourite" >> beam.Filter(lambda row: row['favourite'] is not None)
                #| "debug match " >> beam.Map(print)
        )

        runner_dict = (
                pipeline
                | "Initialize runner extraction" >> beam.Create([{}])
                # Each row is a dictionary where the keys are the BigQuery columns
                | 'Read runner table' >> beam.ParDo(ReadFromDBFn(url=db_url, query=query_runner))
                | "Parse runner row " >> beam.ParDo(RunnerRow())
                #| 'debug runner ' >> beam.Map(print)
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
                #| 'debug draw match ' >> beam.Map(print)
        )

        samples = (
                pipeline
                | 'Create sample pcoll' >> ReadFromText('gs://' + bucket + '/' + start_of_day + '/live/*.json')
                | 'Convert sample file to json' >> ParDo(JsonParser())
                | 'flatten samples ' >> beam.FlatMap(lambda x: x)
                | 'Getting back record' >> beam.Map(lambda sample: aJson(sample))
                | 'Filter empty sample ' >> beam.Filter(lambda sample: bool(sample))
        )

        samples_enriched_with_start_quotes = (
                samples
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
                | 'remove duplicated minute' >> beam.Map(lambda df: df.drop_duplicates(subset='minute', keep='first'))
                #| 'write to csv ' >> beam.Map(lambda df: store_df(df))
                | 'interpolate quote values for missing ts ' >> beam.Map(lambda df: interpolate_missing_ts(df))
                | 'drop rule out goals ' >> beam.Map(lambda df: drop_rule_out_goals(df))
                #| 'add sum_goal column ' >> beam.Map(lambda df: df.assign(sum_goals=lambda row: row.agoal + row.hgoal))
                #| 'add current_result column' >> beam.Map(lambda df: assign_current_result(df))
                | 'add goal_diff_by_prediction column' >> beam.Map(lambda df: assign_goal_diff_by_prediction(df))
                | 'drop draw matches ' >> beam.Filter(lambda df: not is_draw_match(df))
                | 'merge all dataframe ' >> beam.CombineGlobally(lambda dfs: merge_df(dfs))
                | 'select columns ' >> beam.Map(lambda df: df[['event_id','runner_name','minute','prediction','back','lay','start_lay','start_back','draw_perc','goal_diff_by_prediction','score']])
                | 'filter empty dataframe ' >> beam.Filter(lambda df: not df.empty)
                | 'convert df to list of records ' >> beam.FlatMap(lambda df: df.values.tolist())
                | 'csv format ' >> beam.Map(lambda row: ';'.join([str(column) for column in row]))
                | 'write to csv ' >> WriteToText('gs://' + bucket + '/stage/data_' + start_of_day + '.csv', num_shards=0, shard_name_template='', header='event_id,runner_name;minute;prediction;back;lay;start_lay;start_back;draw_perc;goal_diff_by_prediction;score')
            )


        # _ = (pipeline
        #         | 'Create sample pcoll' >> ReadFromText(file_pattern='C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\gcp\\debug\\*.csv', skip_header_lines=1)
        #         | 'split ' >> beam.Map(lambda row: row.split(';'))
        #         | 'assign ev_id as key ' >> WithKeys(lambda row : row[0])
        #         | 'group by key' >> GroupByKey()
        #         | 'get list of rows ' >> beam.Map(lambda tuple: tuple[1])
        #         | 'create dataframe for an event ' >> beam.Map(lambda rows: create_df_by_event(rows))
        #         | ' cast minute to int ' >> beam.Map(lambda df: df.astype({'minute':'int'}))
        #         | ' cast lay to int ' >> beam.Map(lambda df: df.astype({'lay':'float'}))
        #         | ' cast ag to int ' >> beam.Map(lambda df: df.astype({'agoal':'int'}))
        #         | ' cast hg to int ' >> beam.Map(lambda df: df.astype({'hgoal':'int'}))
        #         | 'interpolate quote values for missing ts ' >> beam.Map(lambda df: interpolate_missing_ts(df))
        #         | 'drop rule out goals ' >> beam.Map(lambda df: drop_rule_out_goals(df))
        #         | 'add sum_goal column ' >> beam.Map(lambda df: df.assign(sum_goals=lambda row: row.agoal + row.hgoal))
        #         | 'add current_result column' >> beam.Map(lambda df: assign_current_result(df))
        #         | 'add goal_diff_by_prediction column' >> beam.Map(lambda df: assign_goal_diff_by_prediction(df))
        #         | 'drop draw matches ' >> beam.Filter(lambda df: not is_draw_match(df))
        #         | 'merge all dataframe ' >> beam.CombineGlobally(lambda dfs: merge_df(dfs))
        #         | 'filter empty dataframe ' >> beam.Filter(lambda df: not df.empty)
        #         | 'convert df to list of records ' >> beam.FlatMap(lambda df: df.values.tolist())
        #         | 'csv format ' >> beam.Map(lambda row: ';'.join([str(column) for column in row]))
        #         | 'write to csv ' >> WriteToText('output.csv', file_name_suffix='.csv', header='minute,prediction,back,lay,start_lay,start_back,hgoal,agoal,available,matched,total_available,total_matched,draw_perc')
        #         #| 'write to csv ' >> WriteToText('gs://' + bucket + '/stage/data_' + start_of_day + '.csv')
        #      )

    logging.info("pipeline started")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--db_url',
        dest='db_url',
        required=True,
        help='bucket where read/write csv'
    )

    known_args, pipeline_args = parser.parse_known_args()
    #run(known_args.bucket, pipeline_args)
    run(known_args.db_url, pipeline_args)
