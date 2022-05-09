"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import ast
import json
import logging
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


class MatchRow (DoFn):
    def process(self, element):
        event_id, competition_id, cutoff_date, delta, event_name, favourite, guest, home, outcome = element.split(";")
        if favourite == '':
            return []

        return [{
            'event_id': [event_id],
            'competition_id': [competition_id],
            'cutoff_date': [cutoff_date],
            'event_name': [event_name],
            'delta': [delta],
            'guest': [guest],
            'home': [home],
            'favourite': [favourite]
        }]

class EnrichWithFinalScore (DoFn):
    def process(self, match_dict, scores_tuples):
        scores_dict = dict(scores_tuples)
        match_dict['score'] = [scores_dict[match_dict['event_id'][0]]]
        yield match_dict

def score (tuple):
    key = tuple[0]
    score = 'DRAW'
    samples = reversed(sorted(tuple[1], key=lambda score: score['ts']))
    for sample in samples:
        if bool(sample['home']) and bool(sample['away']):
            if sample['home']['goals'] == sample['away']['goals']:
                score = 'DRAW'
                break
            elif sample['home']['goals'] > sample['away']['goals']:
                score = 'HOME'
                break
            else:
                score = 'AWAY'
                break

    return (key, score)

def merge_df(dfs):
    if not dfs:
        logging.info("No dataframe to concat ")
        return pd.DataFrame([])

    return pd.concat(dfs).reset_index(drop=True)


def run(match_csv, out_csv, args=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        args, streaming=True, save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:

        scores_dict = (
                pipeline
                | "Matching stats" >> fileio.MatchFiles('C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\stats\\*.json')
                | "Reading stats " >> fileio.ReadMatches()
                | "Convert stats file to json" >> JsonReader()
                | "Flatten stats " >> beam.FlatMap(lambda x: x)
                | "Add key to stats " >> WithKeys(lambda x: x['eventId'])
                | "group by key" >> GroupByKey()
                | "get last score " >> beam.Map(lambda tuple: score(tuple))
        )

        _ = (
                pipeline
                | "Read matches " >> beam.io.ReadFromText("csv\\" + match_csv, skip_header_lines=1)
                | "Parse match row " >> beam.ParDo(MatchRow())
                | "Filter matches without favourite" >> beam.Filter(lambda row: len(row) > 0)
                | "Enrich sample with start quotes" >> beam.ParDo(EnrichWithFinalScore(), beam.pvalue.AsList(scores_dict))
                | "convert to df " >> beam.Map(lambda dict: pd.DataFrame.from_dict(dict))
                | "merge dfs " >> beam.CombineGlobally( lambda dfs: merge_df(dfs))
                | 'write to csv ' >> beam.Map(lambda df: df.to_csv('csv_enrich/' + out_csv, sep=';', index=False, encoding="utf-8", line_terminator='\n'))
        )

    logging.info("pipeline started")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--match_csv',
        dest='match_csv',
        required=True,
        help='csv with the match to consider'
    )

    parser.add_argument(
        '--out_csv',
        dest='out_csv',
        required=True,
        help='csv with the output of the pipeline'
    )

    known_args, pipeline_args = parser.parse_known_args()
    run(known_args.match_csv, known_args.out_csv, pipeline_args)