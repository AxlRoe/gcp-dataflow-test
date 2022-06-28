"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import json
import logging
import math
from collections import Counter
from datetime import datetime, time, timedelta

import apache_beam as beam
import jsonpickle
import numpy as np
import pandas as pd
from apache_beam import DoFn, ParDo, WithKeys, GroupByKey
from apache_beam.io import WriteToText, ReadFromText
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions

class RunnerRow (DoFn):
    def process(self, element):
        return [{
            'id': element['id'],
            'back': float(element['back']),
            'lay': float(element['lay'])
        }]

def run(args=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        args, save_main_session=True
    )

    bucket = 'dump-bucket-4'
    with beam.Pipeline(options=pipeline_options) as pipeline:

        runner_table_spec = bigquery.TableReference(projectId='scraper-v1-351921', datasetId='bet', tableId='runner')
        _ = (
                pipeline
                # Each row is a dictionary where the keys are the BigQuery columns
                | 'Read runner bq table' >> beam.io.ReadFromBigQuery(gcs_location='gs://' + bucket + '/tmp/', table=runner_table_spec)
                | "Parse runner row " >> beam.ParDo(RunnerRow())
                | 'debug runner ' >> beam.Map(print)
        )


    logging.info("pipeline started")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    # parser.add_argument(
    #     '--bucket',
    #     dest='bucket',
    #     required=True,
    #     help='bucket where read/write csv'
    # )

    known_args, pipeline_args = parser.parse_known_args()
    #run(known_args.bucket, pipeline_args)
    run(pipeline_args)