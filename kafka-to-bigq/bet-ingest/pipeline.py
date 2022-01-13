"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import ast
import json
import logging
import random
from pathlib import Path

import apache_beam as beam
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


class WriteDFToFile(beam.DoFn):

    def process(self, mylist):

        if not mylist:
            return

        header = ['id', 'ts', 'delta', 'prediction', 'back', 'lay', 'start_back', 'start_lay', 'hgoal', 'agoal', 'runner_name', 'event_name',
                  'event_id', 'market_name']
        # header = ["exchangeId", "ts", "back", "lay", "backDiff", "layDiff", "home", "guest", "runnerName", "eventName", "eventId", "marketName"]
        mylist.insert(0, header)
        df = pd.DataFrame(mylist[1:], columns=mylist[0])

        file_exists = Path("data.csv").exists()
        df.to_csv("data.csv", sep=';', index=False, header=not file_exists, mode='a' if file_exists else 'w')

        # logging.info("dump to csv")


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
            sample_json["delta"],
            sample_json["prediction"],
            sample_json["back"],
            sample_json["lay"],
            sample_json["startLay"],
            sample_json["startBack"],
            sample_json["hgoal"],
            sample_json["agoal"],
            sample_json["runnerName"],
            sample_json["eventName"],
            sample_json["eventId"],
            sample_json["marketName"],
        ]

    class WriteDFToFile2(beam.DoFn):

        def process(self, json):
            header = ['id', 'ts', 'back', 'lay', 'back_diff', 'lay_diff', 'hgoal', 'agoal', 'runner_name', 'event_name',
                      'event_id', 'market_name']

            record = toRecord(json)
            records = []
            records.insert(0, header)
            records.insert(1, record)
            df = pd.DataFrame(records[1:], columns=records[0])

            file_exists = Path("tmp.csv").exists()
            df.to_csv("tmp.csv", sep=';', index=False, header=not file_exists, mode='a' if file_exists else 'w')

            # logging.info("dump to csv")

    def aJson(stats, sample):
        return {
            "exchangeId": sample["exchangeId"],
            "ts": sample["ts"],
            "delta": None,
            "prediction": None,
            "back": sample["back"],
            "lay": sample["lay"],
            "startLay": None,
            "startBack": None,
            "home": sample["home"],
            "hgoal": stats["home"]["goals"],
            "guest": sample["guest"],
            "agoal": stats["away"]["goals"],
            "runnerName": sample["runnerName"],
            "eventName": sample["eventName"],
            "eventId": sample["eventId"],
            "marketName": sample["marketName"],
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
                print("missing values for stats, event: " + sample["exchangeId"])
                continue

            output.append(aJson(stats, sample))

        return output

    def tuple_to_jsons(merged_tuple):
        data = merged_tuple[1]

        prematch_sample = data["prematch"][0]
        sample_and_goals = list(filter(
            lambda sg: sg['runnerName'] == prematch_sample['runnerName']
                       and sg['eventId'] == prematch_sample['eventId'], data["sample_and_goal"]))

        output = []
        for sample_and_goal in sample_and_goals:
            pre_back = prematch_sample['back'] if prematch_sample['back'] >= 0 else None
            lay = sample_and_goal['lay'] if sample_and_goal['lay'] >= 0 else None
            # layDiff = round(lay - pre_back, 2) if lay is not None and pre_back is not None else None

            pre_lay = prematch_sample['lay'] if prematch_sample['lay'] >= 0 else None
            back = sample_and_goal['back'] if sample_and_goal['back'] >= 0 else None
            # backDiff = round(back - pre_lay, 2) if back is not None and pre_lay is not None else None

            sample_and_goal['lay'] = lay
            sample_and_goal['back'] = back
            sample_and_goal['startLay'] = pre_lay
            sample_and_goal['startBack'] = pre_back

            output.append(sample_and_goal)

        return output


    def records(merged_tuple):

        data = merged_tuple[1]
        pre_sample_and_goals = data["pre_samples_and_goal"]

        player_1 = {}
        player_2 = {}

        if len(data["players"]) < 2:
            logging.warn("Missing players for " + merged_tuple[0])
            return []

        if data["players"][0]['home'] == data["players"][0]['runnerName']:
            player_1 = data["players"][0]

        if data["players"][0]['guest'] == data["players"][0]['runnerName']:
            player_2 = data["players"][0]

        if data["players"][1]['home'] == data["players"][1]['runnerName']:
            player_1 = data["players"][1]

        if data["players"][1]['guest'] == data["players"][1]['runnerName']:
            player_2 = data["players"][1]

        delta = abs(player_1['lay'] - player_2['lay'])
        prediction = 'HOME' if player_1['lay'] < player_2['lay'] else 'AWAY'

        output = []
        for pre_sample_and_goal in pre_sample_and_goals:
            pre_sample_and_goal['prediction'] = prediction
            pre_sample_and_goal['delta'] = delta
            if pre_sample_and_goal['marketName'] == 'MATCH_ODDS':
                if pre_sample_and_goal['runnerName'] != 'Pareggio':
                    pre_sample_and_goal['runnerName'] = 'HOME' if pre_sample_and_goal['runnerName'] == pre_sample_and_goal['home'] else 'AWAY'

            output.append(toRecord(pre_sample_and_goal))

        return output

    def debug_filter(prematch_tuple):
        json = prematch_tuple[1]
        if json['eventId'] == '31134143' and json['marketName'] == 'MATCH_ODDS':
            print("************ Found " + str(prematch_tuple[0]))
            return True

        return False

    def home_or_guest_filter(prematch_tuple):
        json = prematch_tuple[1]
        return json['marketName'] == 'MATCH_ODDS' and json['runnerName'] != 'Pareggio'

    with beam.Pipeline() as pipeline:
        samples_tuple = (
                pipeline
                | "Matching samples" >> fileio.MatchFiles(
            'C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\samples\\*.json')
                | "Reading sampling" >> fileio.ReadMatches()
                | "Convert sample file to json" >> JsonReader()
                | "Flatten samples " >> beam.FlatMap(lambda x: x)
                | "map samples " >> beam.Map(lambda x: x)
                | "Add key to samples " >> WithKeys(lambda x: x['eventId'] + '#' + x['ts'])
        )

        stats_tuple = (
                pipeline
                | "Matching stats" >> fileio.MatchFiles(
            'C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\stats\\*.json')
                | "Reading stats " >> fileio.ReadMatches()
                | "Convert stats file to json" >> JsonReader()
                | "Flatten stats " >> beam.FlatMap(lambda x: x)
                | "map stats " >> beam.Map(lambda x: x)
                | "Add key to stats " >> WithKeys(lambda x: x['eventId'] + '#' + x['ts'])
        )

        prematch_tuples = (
                pipeline
                | "Getting prematch files" >> fileio.MatchFiles(
            'C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\prematch\\*.json')
                | "Reading prematch files" >> fileio.ReadMatches()
                | "Converting prematch files to json" >> JsonReader()
                | "Flatten prematch " >> beam.FlatMap(lambda x: x)
                | "map prematch " >> beam.Map(lambda x: x)
                | "add key using runner name " >> WithKeys(lambda x: x['eventId'] + '#' + x['runnerName'])
        )

        # d = (
        #         prematch_tuples
        #         | "debug" >> beam.Filter(lambda prematch_tuple: debug_filter(prematch_tuple))
        #         | "get player" >> beam.Map(lambda prematch_tuple: prematch_tuple[1])
        #         | 'To string' >> beam.ToString.Kvs()
        #         | beam.Map(print)
        # )

        player_tuples = (
                prematch_tuples
                | "Filter match odds values" >> beam.Filter(lambda prematch_tuple: home_or_guest_filter(prematch_tuple))
                | "get player jsons from tuples" >> beam.Map(lambda prematch_tuple: prematch_tuple[1])
                | "Player tuples " >> WithKeys(lambda player_json: player_json['eventId'])
        )

        # sample_with_goal_tuples = (
        #         ({'samples': samples_tuple, 'stats': stats_tuple})
        #         | 'Merge back record' >> beam.CoGroupByKey()
        #         | 'Getting back record' >> beam.FlatMap(lambda x: sample_and_goal_jsons(x))
        #         # | "add key " >> WithKeys(lambda x: x['eventId'] + '#' + x['runnerName'])
        #         | 'Write to csv' >> beam.ParDo(WriteDFToFile2())
        #     # | beam.Map(lambda sample: beam.Row(id=sample["id"], ts=sample["ts"], quote=sample["quote"], home=sample["home"], away=sample["away"], runner_name=sample["runnerName"], event_name=sample["eventName"], market_name=sample["marketName"]))
        # )

        sample_with_goal_tuples = (
                ({'samples': samples_tuple, 'stats': stats_tuple})
                | 'Merge back record' >> beam.CoGroupByKey()
                | 'remove empty stats ' >> beam.Filter(lambda merged_tuple: len(merged_tuple[1]['samples']) > 0 and len(merged_tuple[1]['stats']) > 0)
                | 'Getting back record' >> beam.FlatMap(lambda x: sample_and_goal_jsons(x))
                | "add key " >> WithKeys(lambda x: x['eventId'] + '#' + x['runnerName'])
        )

        pre_samples_goal_tuples = (
                ({'prematch': prematch_tuples, 'sample_and_goal': sample_with_goal_tuples})
                | 'Merge by eventId and runnerName' >> beam.CoGroupByKey()
                | 'Remove empty prematch key' >> beam.Filter(lambda merged_tuple: len(merged_tuple[1]['prematch']) > 0 and len(merged_tuple[1]['sample_and_goal']) > 0)
                | 'convert join tuple to json ' >> beam.FlatMap(lambda merged_tuple: tuple_to_jsons(merged_tuple))
                | "Add key to join between pre/live/scores " >> WithKeys(lambda merged_json: merged_json['eventId'])
        )

        (
                ({'players': player_tuples, 'pre_samples_and_goal': pre_samples_goal_tuples})
                | 'Merge by eventId' >> beam.CoGroupByKey()
                | 'Convert join to records' >> beam.Map(lambda x: records(x))
                | 'Write to csv' >> beam.ParDo(WriteDFToFile())
        )

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

    # print(dateutil.parser.isoparse('2021-11-03T17:22:59.5850463'))
