"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import ast
import json
import logging
import random
from pathlib import Path

import apache_beam as beam
from apache_beam import DoFn, ParDo, WithKeys, GroupByKey
from apache_beam.dataframe.convert import to_dataframe
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

def aJson(stats, sample):
    return {
        "exchangeId": sample["exchangeId"],
        "runnerId": str(sample["runnerId"]),
        "ts": sample["ts"],
        "delta": float("NaN"),
        "prediction": None,
        "back": round(sample["back"] * 100) / 100,
        "lay": round(sample["lay"] * 100) / 100,
        "startLay": float("NaN"),
        "startBack": float("NaN"),
        "home": sample["home"],
        "hgoal": stats["home"]["goals"],
        "guest": sample["guest"],
        "agoal": stats["away"]["goals"],
        "runnerName": sample["runnerName"],
        "eventName": sample["eventName"],
        "eventId": sample["eventId"],
        "marketName": sample["marketName"],
        "marketId": sample["marketId"],
        "totalAvailable": round(sample["totalAvailable"] * 100) / 100,
        "totalMatched": round(sample["totalMatched"] * 100) / 100,
        "matched": round(sample["matched"] * 100) / 100,
        "available": round(sample["available"] * 100) / 100,
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

class MatchRow (DoFn):
    def process(self, element):
        event_id, competition_id, cutoff_date, delta, event_name, favourite, guest, home = element.split(";")
        return [{
            'event_id': event_id,
            'competition_id': competition_id,
            'cutoff_date': cutoff_date,
            'event_name': event_name,
            'guest': guest,
            'home': home,
            'favourite' : favourite,
            'delta' : delta
        }]

class RunnerRow (DoFn):
    def process(self, element):
        id, market_id, runner_id, available, back, lay, market_name, matched, runner_name, total_available, total_matched = element.split(";")
        return [{
            'id': id,
            'runner_id': runner_id,
            'available': available,
            'back': back,
            'lay': lay,
            'market_id': market_id,
            'market_name': market_name,
            'matched': matched,
            'runner_name': runner_name,
            'total_available': total_available,
            'total_matched': total_matched
        }]

class EnrichWithStartQuotes (DoFn):
    def process(self, tuple, runners):
        sample = tuple[1]
        runner_dict = {x['id'] + '_' + x['runner_id'] + '#' + x['market_name']: x for x in filter(lambda runner: runner['id'] == sample['eventId'], runners)}
        key = sample['exchangeId']

        if key in runner_dict.keys():
            if runner_dict[key]:
                runner = runner_dict[key]
                sample['startLay'] = runner['lay']
                sample['startBack'] = runner['back']
        else:
            logging.warn("Missing " + key + " in runner table ")
            yield {}

        yield sample

class EnrichWithPrediction (DoFn):
    def process (self, tuple, matches):

        sample = tuple[1]
        match_dict = {x['event_id']: x for x in filter(lambda match: match['event_id'] == sample['eventId'], matches)}
        key = sample['eventId']

        if key in match_dict.keys():
            if match_dict[key]:
                match = match_dict[key]
                sample['prediction'] = match['favourite']
                sample['delta'] = match['delta']
        else:
            logging.warn("Missing " + key + " in match table ")
            yield {}

        yield sample #toRecord(sample)

def run(bootstrap_servers, match_csv, runner_csv, args=None):
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

    with beam.Pipeline(options=pipeline_options) as pipeline:

        match_dict = (
                pipeline
                | "Read matches " >> beam.io.ReadFromText("csv\\" + match_csv, skip_header_lines=1)
                | "Parse match row " >> beam.ParDo(MatchRow())

        )
        runner_dict = (
                pipeline
                | "Read runners " >> beam.io.ReadFromText("csv\\" + runner_csv, skip_header_lines=1)
                | "Parse runner row " >> beam.ParDo(RunnerRow())
        )

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

        sample_with_score_tuples = (
                ({'samples': samples_tuple, 'stats': stats_tuple})
                | 'Merge back record' >> beam.CoGroupByKey()
                | 'remove empty stats ' >> beam.Filter(lambda merged_tuple: len(merged_tuple[1]['samples']) > 0 and len(merged_tuple[1]['stats']) > 0)
                | 'Getting back record' >> beam.FlatMap(lambda x: sample_and_goal_jsons(x))
                | "add key " >> WithKeys(lambda x: x['eventId'] + '#' + x['runnerId'])
        )

        pre_samples_goal_tuples = (
                sample_with_score_tuples
                | 'Enrich sample with start quotes' >> beam.ParDo(EnrichWithStartQuotes(), beam.pvalue.AsList(runner_dict))
                | 'Remove empty sample for missing runner ' >> beam.Filter(lambda sample: bool(sample))
                | "Add key to join between pre/live/scores " >> WithKeys(lambda merged_json: merged_json['eventId'])
        )

        df = to_dataframe(pre_samples_goal_tuples
                | 'Enrich sample with home and guest ' >> beam.ParDo(EnrichWithPrediction(), beam.pvalue.AsList(match_dict))
                | 'Remove empty sample for missing match ' >> beam.Filter(lambda sample: bool(sample))
                | 'Define schema' >> beam.Select(
                    id=lambda x: str(x['exchangeId']),
                    runner_id=lambda x: str(x['runnerId']),
                    ts=lambda x: str(x['ts']),
                    delta=lambda x: str(x['delta']),
                    prediction=lambda x: str(x['prediction']),
                    back=lambda x: float(x['back']),
                    lay=lambda x: float(x['lay']),
                    start_lay=lambda x: float(x['startLay']),
                    start_back=lambda x: float(x['startBack']),
                    hgoal=lambda x: str(x['hgoal']),
                    agoal=lambda x: str(x['agoal']),
                    runner_name=lambda x: str(x['runnerName']),
                    event_name=lambda x: str(x['eventName']),
                    event_id=lambda x: str(x['eventId']),
                    market_name=lambda x: str(x['marketName']),
                    available=lambda x: float(x['available']),
                    matched=lambda x: float(x['matched']),
                    total_available=lambda x: float(x['totalAvailable']),
                    total_matched=lambda x: float(x['totalMatched']))
            )

        file_exists = Path("data.csv").exists()
        df.to_csv("data.csv", sep=';', index=False, header=not file_exists, mode='a' if file_exists else 'w', encoding="utf-8", line_terminator='\n')

    logging.info("pipeline started")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bootstrap_servers',
        dest='bootstrap_servers',
        required=True,
        help='Bootstrap servers for the Kafka cluster. Should be accessible by the runner'
    )
    parser.add_argument(
        '--match_csv',
        dest='match_csv',
        required=True,
        help='csv with the match to consider'
    )
    parser.add_argument(
        '--runner_csv',
        dest='runner_csv',
        required=True,
        help='csv with the runner to consider'
    )

    known_args, pipeline_args = parser.parse_known_args()
    run(known_args.bootstrap_servers, known_args.match_csv, known_args.runner_csv, pipeline_args)

    # print(dateutil.parser.isoparse('2021-11-03T17:22:59.5850463'))


# https://stackoverflow.com/questions/70520233/concatenating-multiple-csv-files-in-apache-beam/70534957
# class convert_to_dataFrame(beam.DoFn):
#     def process(self, element):
#         return pd.DataFrame(element)
#
# class merge_dataframes(beam.DoFn):
#     def process(self, element):
#         logging.info(element)
#         logging.info(type(element))
#         return pd.concat(element).reset_index(drop=True)
#
# p = beam.Pipeline()
# concating = (p
#              | beam.io.fileio.MatchFiles("C:/Users/firuz/Documents/task/mobilab_da_task/concats/**")
#              | beam.io.fileio.ReadMatches()
#              | beam.Reshuffle()
#              | beam.ParDo(convert_to_dataFrame())
#              | beam.combiners.ToList()
#              | beam.ParDo(merge_dataframes())
#              | beam.io.WriteToText('C:/Users/firuz/Documents/task/mobilab_da_task/output_tests/merged', file_name_suffix='.csv'))
#
# p.run()