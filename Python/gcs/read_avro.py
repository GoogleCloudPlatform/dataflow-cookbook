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
import logging

# third party libraries
import apache_beam as beam
from apache_beam import Map
from apache_beam.io.avroio import ReadFromAvro
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
    class ReadAvroOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                "--path",
                default="gs://cloud-samples-data/bigquery/us-states/*.avro",
                help="GCS path to read from",
            )

    options = ReadAvroOptions()

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadAvro" >> ReadFromAvro(options.path)
            | "CheckRow"
            >> Map(
                lambda row: f"The abbreviation of {row['name']} is {row['post_abbr']}"  # noqa:E501
            )
            | Map(logging.info)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
