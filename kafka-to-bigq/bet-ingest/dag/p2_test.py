"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam import DoFn, ParDo
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions


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

def run(args=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        args, save_main_session=True
    )

    bucket = 'dump-bucket-4'
    with beam.Pipeline(options=pipeline_options) as pipeline:

        match_table_spec = bigquery.TableReference(projectId='scraper-v1-351921', datasetId='bet', tableId='match')
        _ = (
                pipeline
                # Each row is a dictionary where the keys are the BigQuery columns
                | 'Read match bq table' >> beam.io.ReadFromBigQuery(gcs_location='gs://' + bucket + '/tmp/', table=match_table_spec)
                | "Convert list in row " >> ParDo(MatchRow())
                | "Filter matches without favourite" >> beam.Filter(lambda row: row['favourite'] is not None)
                | "debug match " >> beam.Map(print)
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
    run(pipeline_args)