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


def make_row(element):
    return {
        "ride_status": element["ride_status"],
        "passenger_count": element["passenger_count"],
        "meter_reading": element["meter_reading"],
        "timestamp": element["timestamp"],
    }


def run(argv=None):
    topic = "projects/pubsub-public-data/topics/taxirides-realtime"

    class StreamingInsertsOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument("--output_table", help="BQ Table to write")

    table_schema = "ride_status:STRING, passenger_count:INTEGER, meter_reading:FLOAT, timestamp:STRING"
    options = StreamingInsertsOptions()
    with beam.Pipeline(options=options) as p:
        output = (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(topic=topic)
            | "Load Json" >> Map(json.loads)
            | "Select Fields" >> Map(make_row)
            # Defaults to StreamingInserts
            | WriteToBigQuery(
                options.output_table,
                schema=table_schema,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
