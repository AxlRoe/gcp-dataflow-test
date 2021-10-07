#!/usr/bin/env python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An Apache Beam streaming pipeline example.

It reads JSON encoded messages from Pub/Sub, transforms the message data and
writes the results to BigQuery.
"""

import argparse
import json
import logging
from typing import Any, Dict, List

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io import kafka
from apache_beam.options.pipeline_options import PipelineOptions

# Defines the BigQuery schema for the output table.
SCHEMA = ",".join(
    [
        "id:STRING",
        "market_name:INTEGER",
        "runner_name:STRING",
        "lay:NUMERIC",
        "back:NUMERIC",
        "ts:TIMESTAMP",
    ]
)

def parse_json_message(message: str) -> Dict[str, Any]:
    """TODO Convert json to dataframe and get dict """
    """Parse the input json message and add 'score' & 'processing_time' keys."""
    row = json.loads(message)
    return {
        "id": row["id"],
        "ts": row["ts"],
        "market_name": row["market_name"],
        "runner_name": row["runner_name"],
        "lay": float(row["lay"]),
        "back": float(row["back"])
    }


def run(
    window_interval_sec: int = 60,
    beam_args: List[str] = None,
) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)

    with beam.Pipeline(options=options) as pipeline:
        messages = (
            pipeline
            | "Read from Kafka"
            >> kafka.ReadFromKafka(consumer_config = {'bootstrap_servers': 'localhost:9092'},
                topics=['exchange.samples'],
                key_deserializer='org.apache.kafka.common.serialization.StringDeserializer',
                value_deserializer='org.apache.kafka.common.serialization.StringDeserializer'
            ).with_output_types(bytes)
            | "UTF-8 bytes to string" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "Parse JSON messages" >> beam.Map(parse_json_message)
            | "Fixed-size windows"
            >> beam.WindowInto(window.FixedWindows(window_interval_sec, 0))
        )

        # Output the results into BigQuery table.
        _ = messages | "Write to Big Query" >> beam.io.WriteToBigQuery(
            'kafka_to_bigquery.transactions', schema=SCHEMA
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    args, beam_args = parser.parse_known_args()

    run(
        window_interval_sec=args.window_interval_sec,
        beam_args=beam_args,
    )
