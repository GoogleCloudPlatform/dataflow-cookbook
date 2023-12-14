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
import apache_beam as beam

# third party libraries
from apache_beam import Map
from apache_beam.io.gcp.bigtableio import ReadFromBigtable
from apache_beam.options.pipeline_options import PipelineOptions


class BigtableOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--project_id",
            default="your-project-id",
            help="Project ID"
        )
        parser.add_argument(
            "--instance_id",
            default="beam-test",
            help="Cloud Bigtable instance ID"
        )
        parser.add_argument(
            "--table_id",
            default="your-test-table",
            help="Cloud Bigtable table ID"
        )


def run():
    """
    This pipeline shows how to read from Cloud Bigtable.
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
            | "Extract cells" >> beam.Map(
                lambda row: row._cells
            )
            | "Log Data" >> Map(logging.info)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
