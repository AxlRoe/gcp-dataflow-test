"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import ast
import json
import logging
import random
from pathlib import Path

import numpy as np
import pandas as pd
import apache_beam as beam
from apache_beam import DoFn, ParDo, Pipeline, WithKeys, GroupByKey, Row
from apache_beam.io import fileio, ReadFromPubSub
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
from apache_beam.dataframe.convert import to_dataframe

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

class WriteDFToFile(beam.DoFn):

    def process(self, mylist):
        header = ["exchangeId", "ts", "back", "lay", "backDiff", "layDiff", "home", "away", "runnerName", "eventName", "eventId", "marketName"]
        mylist.insert(0, header)
        df = pd.DataFrame(mylist[1:], columns=mylist[0])

        file_exists = Path("data.csv").exists()
        df.to_csv("data.csv", index=False, header=not file_exists, mode='a' if file_exists else 'w')

        #logging.info("dump to csv")

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
        #yield json.loads('{"id": "1", "market_name" : "test", "runner_name" : "test", "ts" : "2021-10-05T15:50:00.890Z", "lay": 1.0, "back" : 1.0}')
        data = file.read_utf8()
        if not data:
            logging.info("Json read is null")
            yield json.loads('{}')

        sample = json.loads(data)
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

def run(bootstrap_servers, args=None):
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

    def toRecord(sample_json):
        return [
            sample_json["exchangeId"],
            sample_json["ts"],
            sample_json["back"],
            sample_json["lay"],
            sample_json["layDiff"],
            sample_json["backDiff"],
            sample_json["home"],
            sample_json["away"],
            sample_json["runnerName"],
            sample_json["eventName"],
            sample_json["eventId"],
            sample_json["marketName"],
        ]

    def aJson(stats, sample):
        return {
            "exchangeId": sample["exchangeId"],
            "ts": sample["ts"],
            "back": sample["back"],
            "lay": sample["lay"],
            "backDiff": -1,
            "layDiff": -1,
            "home": stats["home"]["goals"],
            "away": stats["away"]["goals"],
            "runnerName": sample["runnerName"],
            "eventName": sample["eventName"],
            "eventId": sample["eventId"],
            "marketName": sample["marketName"],
        }

    def sample_and_goal_jsons(merged_tuple):
        data = merged_tuple[1]
        stats = data["stats"][0]
        samples = data["samples"]
        output = []
        for sample in samples:
            output.append(aJson(stats, sample))

        return output

    def records(merged_tuple):
        data = merged_tuple[1]
        prematch_samples = data["prematch"]
        sample_and_goals = data["sample_and_goal"]
        cartesian_product = np.transpose([np.tile(sample_and_goals, len(prematch_samples)), np.repeat(prematch_samples, len(sample_and_goals))])

        output = []
        for values in cartesian_product:
            sample_and_goal= values[0]
            prematch_sample = values[1]
            layDiff = round(sample_and_goal['lay'] - prematch_sample['back'], 2)
            backDiff = round(sample_and_goal['back'] - prematch_sample['lay'], 2)
            sample_and_goal['layDiff'] = layDiff
            sample_and_goal['backDiff'] = backDiff
            output.append(toRecord(sample_and_goal))

        return output


    with beam.Pipeline() as pipeline:
        samples_tuple = (
                pipeline
                | "Matching samples" >> fileio.MatchFiles('C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\samples\\*.json')
                | "Reading sampling" >> fileio.ReadMatches()
                | "Convert sample file to json" >> JsonReader()
                | "Flatten samples " >> beam.FlatMap(lambda x: x)
                | "Add key to samples " >> WithKeys(lambda x: x['eventId'] + '#' + x['ts'])
        )

        stats_tuple = (
                pipeline
                | "Matching stats" >> fileio.MatchFiles('C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\stats\\*.json')
                | "Reading stats " >> fileio.ReadMatches()
                | "Convert stats file to json" >> JsonReader()
                | "Flatten stats " >> beam.FlatMap(lambda x: x)
                | "Add key to stats " >> WithKeys(lambda x: x['eventId'] + '#' + x['ts'])
        )

        prematch_tuples = (
                pipeline
                | "Getting prematch files" >> fileio.MatchFiles('C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\prematch\\*.json')
                | "Reading prematch files" >> fileio.ReadMatches()
                | "Converting prematch files to json" >> JsonReader()
                | "Flatten prematch " >> beam.FlatMap(lambda x: x)
                | "add key using runner name " >> WithKeys(lambda x: x['eventId'] + '#' + x['runnerName'])
        )


        sample_with_goal_tuples = (
            ({'samples': samples_tuple, 'stats': stats_tuple})
            | 'Merge back record' >> beam.CoGroupByKey()
            | 'Getting back record' >> beam.FlatMap(lambda x: sample_and_goal_jsons(x))
            | "add key " >> WithKeys(lambda x: x['eventId'] + '#' + x['runnerName'])
            #| beam.Map(lambda sample: beam.Row(id=sample["id"], ts=sample["ts"], quote=sample["quote"], home=sample["home"], away=sample["away"], runner_name=sample["runnerName"], event_name=sample["eventName"], market_name=sample["marketName"]))
        )

        (
            ({'prematch': prematch_tuples, 'sample_and_goal': sample_with_goal_tuples})
            | 'Merge by eventId and runnerName' >> beam.CoGroupByKey()
            | 'pippo' >> beam.Map(lambda x: records(x))
            | 'Write to csv' >> beam.ParDo(WriteDFToFile())
        )


    logging.info("pipeline started")


#id;ts;available;away;event_id;home;market_name;matched;quote_diff;runner_name;total_available;total_matched

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bootstrap_servers',
        dest='bootstrap_servers',
        required=True,
        help='Bootstrap servers for the Kafka cluster. Should be accessible by the runner')
    known_args, pipeline_args = parser.parse_known_args()
    run(known_args.bootstrap_servers, pipeline_args)
