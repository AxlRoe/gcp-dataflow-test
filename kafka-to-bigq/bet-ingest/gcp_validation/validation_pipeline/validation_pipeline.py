"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import logging
import math
from decimal import Decimal, ROUND_HALF_UP

import apache_beam as beam
import jsonpickle
import numpy as np
import pandas as pd
from apache_beam import DoFn, WithKeys, GroupByKey
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions


class JsonSink(fileio.TextSink):
    def write(self, record):
        self._fh.write(jsonpickle.encode(record).encode('utf8'))
        self._fh.write('\n'.encode('utf8'))


class Record(DoFn):
    def process(self, element):
        event_id, runner_name, minute, prediction, back, lay, start_lay, start_back, draw_perc, goal_diff_by_prediction = element.split(";")

        odd = None
        start_odd = None
        range = None

        if runner_name == 'DRAW':
            r_start = str(round(start_odd * 2) / 2)
            r_end = str(math.floor(start_odd * 2) / 2)
            range = str(r_start) + '-' + str(r_end)
            start_odd = start_back
            odd = lay

        return [{
            'range': range,
            'event_id': event_id,
            'runner_name': runner_name,
            'odd': float(odd),
            'start_odd': float(start_odd) if start_odd != '' else float('NaN'),
            'minute': minute,
            'goal_diff_by_prediction': float(goal_diff_by_prediction) if goal_diff_by_prediction != '' else float('NaN'),
            'draw_perc': draw_perc
        }]


def run(args=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        args, save_main_session=True
    )

    def calculate_stats(df):
        range = df['range'].iloc[0]
        g_df = df[['odd','minute']].groupby('minute')
        #TODO convert df to dict, for each minute compute sum and count


        pass

    def validate_factory(runner_name):
        if runner_name == 'DRAW':
            return lambda df: extract_sure_draw(df)
        else:
            return None

    def extract_sure_draw(df):
        start_odd = df['runner_name'].iloc[0]
        responsibility = 3 * 0.95 * (start_odd - 1)
        df['revenue'] = df.apply(lambda row: 2 * row.odd - 1, axis=1)
        df['income'] = df.apply(lambda row: row.revenue - responsibility)
        df['is_sure'] = df.apply(lambda row: row.prediction == row.score and row.income >= 1)
        return df[(df['is_sure'] == True)]

    def extract_surebet(df):
        tmp_df = df.sort_values(by=['minute'])
        runner_name = tmp_df['runner_name'].iloc[0]
        validator_fn = validate_factory(runner_name)
        return validator_fn(tmp_df)

    def log_scale_quote(row):
        row['odd'] = round(np.log10(row['odd']) * 100) / 100
        return row

    def create_df_by_event(rows):
        rows.insert(0, ['runner_name', 'odd', 'start_odd', 'goal_diff_by_prediction', 'minute', 'draw_perc'])
        return pd.DataFrame(rows[1:], columns=rows[0])

    bucket = 'dump-bucket-4'
    with beam.Pipeline(options=pipeline_options) as pipeline:
        _ = (
                pipeline
                | "Read csvs " >> beam.io.ReadFromText(file_pattern='gs://' + bucket + '/stage/*.csv', skip_header_lines=1)
                | "Parse record " >> beam.ParDo(Record())
                | "drop rows with nan goal diff " >> beam.Filter(lambda row: not math.isnan(row['start_odd']) and not math.isnan(row['goal_diff_by_prediction']))
                | "Log scale odd quote " >> beam.Map(lambda row: log_scale_quote(row))
                | "Use start_back interval as key " >> WithKeys(lambda row: row['event_id'] + row['runner_name'])
                | "Group sample by event " >> GroupByKey()
                | "Getting back record " >> beam.Map(lambda tuple: pd.DataFrame(tuple[1]))
                | "Extract sure bet points " >> beam.Map(lambda df: extract_surebet(df))
                | "Convert df to list of dict " >> beam.Map(lambda df: df.T.to_dict().values())
                | "Flatten sure bet records " >> beam.FlatMap(lambda x: x)
                | "Associate start odd as key according to runner " >> beam.WithKeys(lambda row: row['range'])
                | "Group by start odd" >> beam.GroupByKey()
                | "Convert dicts to df" >> beam.Map(lambda tuple: pd.DataFrame(tuple[1]))
                | "Calculate validation stats " >> beam.Map(lambda df: calculate_stats(df))
        )

        # _ = (
        #         pipeline
        #         | "Read csvs " >> beam.io.ReadFromText(file_pattern='gs://' + bucket + '/stage/*.csv',
        #                                                skip_header_lines=1)
        #
        #         | 'write to file ' >> fileio.WriteToFiles(
        #     path='gs://' + bucket + '/model/',
        #     destination=lambda model: model['range'],
        #     sink=lambda dest: JsonSink(),
        #     max_writers_per_bundle=1,
        #     shards=1,
        #     file_naming=destination_prefix_naming(suffix='.json'))
        # )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    run(pipeline_args)
