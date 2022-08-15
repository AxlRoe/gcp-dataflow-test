"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import itertools
import json
import logging
import math
from datetime import datetime, time

import apache_beam as beam
import jsonpickle
import numpy as np
import pandas as pd
from apache_beam import DoFn, WithKeys, GroupByKey, ParDo
from apache_beam.io import fileio, ReadFromText
from apache_beam.io.fileio import destination_prefix_naming
from apache_beam.options.pipeline_options import PipelineOptions


class JsonParser(DoFn):
    def process(self, file, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        if not file:
            logging.info("File read is null")
            yield json.loads('{}')

        try:
            model = jsonpickle.decode(file)
            if not model:
                logging.info("Json read is null")
                yield json.loads('{}')

            #logging.info("Parsed json ")
            yield model

        except BaseException as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise

class JsonSink(fileio.TextSink):
    def write(self, record):
        self._fh.write(jsonpickle.encode(record).encode('utf8'))
        self._fh.write('\n'.encode('utf8'))


class Record(DoFn):
    def process(self, element):
        event_id, runner_name, minute, prediction, back, lay, start_lay, start_back, draw_perc, goal_diff_by_prediction, score = element.split(";")

        odd = None
        start_odd = None
        range = None

        if runner_name == 'DRAW':
            start_odd = float(start_back)
            odd = float(lay)
            r_start = str(round(start_odd * 2) / 2)
            r_end = str(math.floor(start_odd * 2) / 2)
            range = str(r_start) + '-' + str(r_end)

        return [{
            'range': range,
            'event_id': event_id,
            'runner_name': runner_name,
            'odd': float(odd),
            'prediction': prediction,
            'start_odd': float(start_odd) if start_odd != '' else float('NaN'),
            'minute': minute,
            'goal_diff_by_prediction': float(goal_diff_by_prediction) if goal_diff_by_prediction != '' else float('NaN'),
            'draw_perc': draw_perc,
            'score': score
        }]




def run(args=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        args, save_main_session=True
    )

    def calculate_error(join_tuple):
        range = join_tuple[0]
        model = join_tuple[1]['model']
        validation = join_tuple[1]['validation']

        if not join_tuple[1]['model'] or not join_tuple[1]['validation']:
            return {}

        bem_model = model['break_even_minute']['mean']

        bem_mean = validation['break_even_minute']['mean']
        bem_uncert = validation['break_even_minute']['uncert']

        bem_error = (bem_model - bem_mean) / bem_model if bem_model - bem_mean > 0 else 0

        error = {
            'range': range,
            'break_even_minute': {
                'error': bem_error,
                'uncert': bem_uncert
            }
        }

        for minute in range(0,120,2):
            key = str(minute)
            pred = model[key]
            real = validation[key]['mean']
            err = abs((real - pred) / real)
            error[key] = {
                'value': err,
                'uncert': validation[key]['uncert']
            }

        return error


    def calculate_surebet_stats(df):
        range = df['range'].iloc[0]
        break_even_minute_mean = df['break_even_minute'].mean()
        break_even_minute_std = df['break_even_minute'].std()
        uncert = round(break_even_minute_std / break_even_minute_mean * 100)

        stats = {
            'range': range,
            'break_even_minute': {
                'mean': break_even_minute_mean,
                'uncert': uncert
            }
        }

        g_df = df[['break_even_minute', 'odd', 'minute']].groupby(['minute'])
        counts = g_df.size().to_frame(name='counts')
        counts_df = (counts
                     .join(g_df.agg({'odd': 'mean'}).rename(columns={'odd': 'odd_mean'}))
                     .join(g_df.agg({'odd': 'var'}).rename(columns={'odd': 'odd_var'}))
                     .reset_index()
                     )

        for index, row in counts_df.iterrows():
            stats[row.minute] = {}
            stats[row.minute]['mean'] = row.odd_mean
            stats[row.minute]['uncert'] = round(row.odd_var / row.odd_mean * 100)

        return stats

    def validate_factory(runner_name):
        if runner_name == 'DRAW':
            return lambda df: extract_sure_draw(df)
        else:
            return None

    def extract_sure_draw(df):
        start_odd = df['start_odd'].iloc[0]
        responsibility = 3 * 0.95 * (start_odd - 1)
        df['revenue'] = df.apply(lambda row: 2 * row.odd - 1, axis=1)
        df['income'] = df.apply(lambda row: row.revenue - responsibility, axis=1)
        df['is_sure'] = df.apply(lambda row: row.prediction == row.score and row.income >= 1, axis=1)
        sb_df = df[(df['is_sure'] == True)]
        break_even_minute = sb_df['minute'].min()
        sb_df = sb_df.assign(break_even_minute=lambda row: break_even_minute)
        return sb_df

    def extract_surebet(df):
        tmp_df = df.sort_values(by=['minute'])
        runner_name = tmp_df['runner_name'].iloc[0]
        validator_fn = validate_factory(runner_name)
        return validator_fn(tmp_df)

    bucket = 'dump-bucket-4'
    with beam.Pipeline(options=pipeline_options) as pipeline:

        model_tuple = (
                pipeline
                | 'Create model pcoll' >> ReadFromText(file_pattern='C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\gcp_validation\\model\\' + '*.json')
                | 'Convert model file to json' >> ParDo(JsonParser())
                | 'Model by range ' >> beam.WithKeys(lambda model: model['range'])
        )

        validation_tuple = (
            pipeline
            #| "Read csvs " >> beam.io.ReadFromText(file_pattern='gs://' + bucket + '/stage/*.csv', skip_header_lines=1)
            | "Read csvs " >> beam.io.ReadFromText(file_pattern='C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\gcp_validation\\validation\\' + '*.csv', skip_header_lines=1)
            | "Parse record " >> beam.ParDo(Record())
            | "drop rows with nan goal diff " >> beam.Filter(lambda row: not math.isnan(row['start_odd']) and not math.isnan(row['goal_diff_by_prediction']))
            | "Use start_back interval as key " >> WithKeys(lambda row: row['event_id'] + row['runner_name'])
            | "Group sample by event " >> GroupByKey()
            | "Getting back record " >> beam.Map(lambda tuple: pd.DataFrame(tuple[1]))
            | "Extract sure bet points " >> beam.Map(lambda df: extract_surebet(df))
            | "filter match with no surebet " >> beam.Map(lambda df: not df.empty)
            | "Convert df to list of dict " >> beam.Map(lambda df: df.T.to_dict().values())
            | "Flatten sure bet records " >> beam.FlatMap(lambda x: x)
            | "Associate start odd as key according to runner " >> beam.WithKeys(lambda row: row['range'])
            | "Group by start odd" >> beam.GroupByKey()
            | "Convert dicts to df" >> beam.Map(lambda tuple: pd.DataFrame(tuple[1]))
            | "Calculate validation stats " >> beam.Map(lambda df: calculate_surebet_stats(df))
            | "Stats by key " >> beam.WithKeys(lambda stats: stats['range'])
        )

        start_of_day = datetime.combine(datetime.utcnow(), time.min)
        _ = (
            ({'model': model_tuple, 'validation': validation_tuple})
            | 'Join model and validation ' >> beam.CoGroupByKey()
            | 'Calculate error ' >> beam.Map(lambda join_tuple: calculate_error(join_tuple))
            | 'write to file ' >> fileio.WriteToFiles(
            path='gs://' + bucket + '/error/',
            destination=lambda error: error['range'] + '_' + str(start_of_day),
            sink=lambda dest: JsonSink(),
            max_writers_per_bundle=1,
            shards=1,
            file_naming=destination_prefix_naming(suffix='.json'))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    run(pipeline_args)
