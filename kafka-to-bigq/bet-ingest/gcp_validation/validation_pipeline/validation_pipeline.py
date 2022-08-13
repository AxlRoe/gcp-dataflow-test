"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import json
import logging
import math
from datetime import datetime, time
from decimal import Decimal, ROUND_HALF_UP

import apache_beam as beam
import jsonpickle
import numpy as np
import pandas as pd
import scipy.stats as st
from apache_beam import DoFn, WithKeys, GroupByKey, ParDo
from apache_beam.io import fileio, ReadFromText
from apache_beam.io.fileio import destination_prefix_naming
from apache_beam.options.pipeline_options import PipelineOptions
from scipy.spatial.distance import cdist
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures


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

class JsonSink(fileio.TextSink):
    def write(self, record):
        self._fh.write(jsonpickle.encode(record).encode('utf8'))
        self._fh.write('\n'.encode('utf8'))

def run(args=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        args, save_main_session=True
    )

    def create_df_by_event(rows):
        rows.insert(0, ['odd', 'goal_diff_by_prediction', 'minute', 'draw_perc'])
        return pd.DataFrame(rows[1:], columns=rows[0])

    start_of_day = datetime.combine(datetime.utcnow(), time.min)
    start_of_day = '2022-08-11'  # start_of_day.strftime("%Y-%m-%d") #'2022-06-27'
    bucket = 'dump-bucket-4'

    with beam.Pipeline(options=pipeline_options) as pipeline:

        samples = (
                pipeline
                | 'Create sample pcoll' >> ReadFromText('gs://' + bucket + '/' + start_of_day + '/live/*.json')
                | 'Convert sample file to json' >> ParDo(JsonParser())
                | 'flatten samples ' >> beam.FlatMap(lambda x: x)
                | "Use start_back interval as key " >> WithKeys(lambda row: row['event_id']) #TODO and runner
                | "Group sample by event " >> GroupByKey()
                | 'Getting back record' >> beam.Map(lambda tuple: create_df_by_event(tuple[1]))
                | 'Filter empty sample ' >> beam.Filter(lambda sample: bool(sample))
        )

        _ = (
                pipeline
                | "Read csvs " >> beam.io.ReadFromText(file_pattern='gs://' + bucket + '/stage/*.csv', skip_header_lines=1)

                | 'write to file ' >> fileio.WriteToFiles(
                                                path='gs://' + bucket + '/model/',
                                                destination=lambda model: model['range'],
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
