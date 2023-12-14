#  Copyright 2023 Google LLC
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
from typing import NamedTuple

# third party libraries
import apache_beam as beam
from apache_beam import coders, Map
from apache_beam.io.gcp.spanner import ReadFromSpanner
from apache_beam.options.pipeline_options import PipelineOptions


class ExampleRow(NamedTuple):
    id: int
    name: str


class SpannerOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--project_id",
            default="your-project-id",
            help="Google Cloud project ID"
        )
        parser.add_argument(
            "--instance_id",
            default="your-instance-id",
            help="Google Cloud Spanner instance ID"
        )
        parser.add_argument(
            "--database_id",
            default="your-database-id",
            help="Google Cloud Spanner database ID"
        )


def run():
    """
    This pipeline shows how to read from Google Cloud Spanner.
    """

    options = SpannerOptions()
    coders.registry.register_coder(ExampleRow, coders.RowCoder)

    with beam.Pipeline(options=options) as p:

        output = (
            p
            | "Read from table" >> ReadFromSpanner(
                project_id=options.project_id,
                instance_id=options.instance_id,
                database_id=options.database_id,
                row_type=ExampleRow,
                sql="SELECT * FROM example_row"
            )
            | "Map Data" >> Map(
                lambda row: f"Id = {row.id}, Name = {row.name}"
            )
            | "Log Data" >> Map(logging.info)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
