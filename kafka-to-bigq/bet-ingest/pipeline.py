"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import ast
import json
import logging
from random import random

import apache_beam as beam
from apache_beam import DoFn, ParDo, Pipeline, WithKeys, GroupByKey
from apache_beam.io import fileio, ReadFromPubSub
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
                | "Read json from storage" >> ParDo(QuoteParser())
        )


class QuoteParser(DoFn):
    def process(self, file, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        data = file.read_utf8()
        if (data is None):
            logging.info("Json read is null")
            yield {}

        yield (json.load(data))


class RecordToGCSBucket(beam.PTransform):
    def expand(self, pcoll):
        return (
                pcoll
                # Bind window info to each element using element timestamp (or publish time).
                | "Window into fixed intervals" >> beam.WindowInto(window.FixedWindows(15, 0))
                | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
                # Group windowed elements by key. All the elements in the same window must fit
                # memory for this. If not, you need to use `beam.util.BatchElements`.
                | "Group by key" >> GroupByKey()
                | "Read event id from message" >> ParDo(EventIdReader())
                | "Read files to ingest " >> beam.FlatMap(lambda event_id: fileio.MatchFiles('gs://data-flow-bucket_1/' + event_id + '/*.json'))
                | "Convert result from match file to readable file " >> fileio.ReadMatches()
                | "shuffle " >> beam.Reshuffle()
                | "Convert file to json" >> JsonReader()
        )


class EventIdReader(DoFn):
    def process(self, tuple):
        k, record = tuple
        # the records have 'value' attribute when --with_metadata is given
        if hasattr(record, 'value'):
            message_bytes = record.value
        elif isinstance(record, tuple):
            message_bytes = record[1]
        else:
            raise RuntimeError('unknown record type: %s' % type(record))
        # Converting bytes record from Kafka to a dictionary.
        message = ast.literal_eval(message_bytes.decode("UTF-8"))
        return message['event_id']


def run(bootstrap_servers, args=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        args, streaming=True, save_main_session=True
    )

    logging.info("kafka address " + bootstrap_servers)
    with Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            # Because `timestamp_attribute` is unspecified in `ReadFromPubSub`, Beam
            # binds the publish time returned by the Pub/Sub server for each message
            # to the element's timestamp parameter, accessible via `DoFn.TimestampParam`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadFromPubSub
            | "Read from Pub/Sub" >> ReadFromPubSub(topic='projects/data-flow-test-327119/topics/exchange.ended.events')
            | "Write to GCS" >> beam.Map(lambda x: logging.info("AHHAHAHAHAHA " + x))
        )

        # (pipeline
        #  | ReadFromKafka(consumer_config={'bootstrap.servers': bootstrap_servers},
        #                  topics=['exchange.ended.events'])
        #  | "Read files " >> RecordToGCSBucket()
        #  | "Write to BigQuery" >> bigquery.WriteToBigQuery(bigquery.TableReference(
        #             projectId='data-flow-test-327119',
        #             datasetId='kafka_to_bigquery',
        #             tableId='transactions'),
        #             schema=SCHEMA,
        #             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        #             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        #  )
    logging.info("pipeline started")


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
