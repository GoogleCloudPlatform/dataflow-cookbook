#  Copyright 2022 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# standard libraries
import json
import logging

# third party libraries
import apache_beam as beam
from apache_beam import Map
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions

INPUT_TOPIC = "projects/pubsub-public-data/topics/taxirides-realtime"


def make_row(element):
    """
    The input elements are passed as a dictionary.
    Select fields 'ride_status', 'passenger_count',
    'meter_reading' and 'timestamp' from elements.
    """
    return {
        "ride_status": element["ride_status"],
        "passenger_count": element["passenger_count"],
        "meter_reading": element["meter_reading"],
        "timestamp": element["timestamp"],
    }


def run(argv=None):
    class StreamingLoadOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                "--output_table", help="BQ Table to write", required=True
            )
            parser.add_argument(
                "--input_topic",
                help='Input PubSub topic of the form "projects/<PROJECT>/topics/<TOPIC>."',  # noqa:E501
                default=INPUT_TOPIC,
            )
            parser.add_argument(
                "--triggering_frequency",
                default=10,
                help="Frequency to trigger the load job",
            )

    table_schema = "ride_status:STRING, passenger_count:INTEGER, meter_reading:FLOAT, timestamp:STRING"  # noqa:E501
    options = StreamingLoadOptions(streaming=True)
    with beam.Pipeline(options=options) as p:
        output = (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(topic=options.input_topic)
            | "Load Json" >> Map(json.loads)
            | "Select Fields" >> Map(make_row)
            | WriteToBigQuery(
                options.output_table,
                schema=table_schema,
                method="FILE_LOADS",
                triggering_frequency=options.triggering_frequency,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
