#  Copyright 2024 Google LLC
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
import apache_beam as beam

# third party libraries
from apache_beam import Map
from apache_beam.io.gcp.bigtableio import ReadFromBigtable
from apache_beam.io.textio import WriteToText
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
        parser.add_argument(
            "--output_file_path",
            required=True,
            help="Output file path to write results to.",
        )


def run():
    """
    This pipeline demonstrates reading from Cloud Bigtable with row filtering
    and key range specification. After reading, all rows are converted to
    strings and then written to the output .txt files.
    """

    options = BigtableOptions()

    with beam.Pipeline(options=options) as p:

        output = (
            p
            | "Read from Bigtable" >> ReadFromBigtable(
                project_id=options.project_id,
                instance_id=options.instance_id,
                table_id=options.table_id
            )
            | 'Filter Rows' >> beam.Filter(filter_rows)
            | "Map to string" >> Map(map_row_to_str)
            | "Write Files" >> WriteToText(options.output_file_path)
        )


def filter_rows(row):
    """
    Filters rows based on the specific condition.
    """
    # Example condition
    return "beam_key" in row.row_key.decode('utf-8')


def map_row_to_str(row):
    """
    Converts a given input BigTable row object into a string.
    """
    # third-party libraries
    from datetime import datetime

    # Example transformation
    row_dict = row.to_dict()
    format_string = f"Row: {row.row_key.decode('utf-8')}"

    for key, cells_list in row_dict.items():
        format_string += f"\n\t Column: {key}"
        for cell in cells_list:
            timestamp = datetime.fromtimestamp(
                cell.timestamp_micros // 1000000
            )
            timestamp_str = timestamp.strftime("%Y/%m/%d %H:%M:%S")
            cell_value = cell.value.decode('utf-8')
            format_string += f"\n\t\t Cell: {cell_value} at {timestamp_str}"
    return [format_string]


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
