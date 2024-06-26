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
from apache_beam import coders, Create, Map
from apache_beam.io.gcp.spanner import SpannerUpdate
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
            required=True,
            help="Google Cloud project ID"
        )
        parser.add_argument(
            "--instance_id",
            required=True,
            help="Google Cloud Spanner instance ID"
        )
        parser.add_argument(
            "--database_id",
            required=True,
            help="Google Cloud Spanner database ID"
        )
        parser.add_argument(
            "--table",
            required=True,
            help="Google Cloud Spanner table name"
        )


def run():
    """
    This pipeline shows how to write update mutations to the Google Cloud
    Spanner table.
    """

    options = SpannerOptions()
    coders.registry.register_coder(ExampleRow, coders.RowCoder)

    with beam.Pipeline(options=options) as p:
        # Create sample data
        elements = [
            (1, "Charles"),
            (2, "Alice"),
            (3, "Bob"),
            (4, "Amanda"),
            (5, "Alex"),
            (6, "Eliza")
        ]

        # Create PCollection from the list
        output = (
            p
            | "Create" >> Create(elements)
            | "Map to Spanner Row"
            >> Map(make_spanner_row).with_output_types(ExampleRow)
            | "Spanner Update" >> SpannerUpdate(
                project_id=options.project_id,
                instance_id=options.instance_id,
                database_id=options.database_id,
                table=options.table
            )
        )


def make_spanner_row(element):
    """
    Converts a given element into an Example Row object.
    """
    return ExampleRow(element[0], element[1])


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
