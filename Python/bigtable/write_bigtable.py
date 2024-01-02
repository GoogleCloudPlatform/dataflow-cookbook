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

# third party libraries
import apache_beam as beam
from apache_beam import Create, Map
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from apache_beam.options.pipeline_options import PipelineOptions


class BigtableOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--project_id",
            required=True,
            help="Project ID"
        )
        parser.add_argument(
            "--instance_id",
            required=True,
            help="Cloud Bigtable instance ID"
        )
        parser.add_argument(
            "--table_id",
            required=True,
            help="Cloud Bigtable table ID"
        )


def run():
    """
    This pipeline shows how to write to Cloud Bigtable.
    """

    options = BigtableOptions()

    with beam.Pipeline(options=options) as p:
        # Create sample data
        elements = [
            (1, "Charles, 1995, USA"),
            (2, "Alice, 1997, Spain"),
            (3, "Bob, 1995, USA"),
            (4, "Amanda, 1991, France"),
            (5, "Alex, 1999, Mexico"),
            (6, "Eliza, 2000, Japan")
        ]

        output = (
            p
            | "Create elements" >> Create(elements)
            | "Map to BigTable Row" >> Map(make_bigtable_row)
            | "Write to BigTable" >> WriteToBigTable(
                project_id=options.project_id,
                instance_id=options.instance_id,
                table_id=options.table_id
            )
        )


def make_bigtable_row(element):
    """
    Converts a given input element into a BigTable DirectRow object.
    """

    # third party libraries
    from google.cloud.bigtable.row import DirectRow
    from datetime import datetime

    # Example transformation to BigTable DirectRow

    index = element[0]
    row_fields = element[1].split(", ")
    column_family_id = "cf1"
    key = "beam_key%s" % ("{0:07}".format(index))
    direct_row = DirectRow(row_key=key)
    for column_id, value in enumerate(row_fields):
        direct_row.set_cell(
            column_family_id,
            ("field%s" % column_id).encode("utf-8"),
            value,
            datetime.now()
        )
    return direct_row


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
