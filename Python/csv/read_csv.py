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
from apache_beam import Map
from apache_beam.io.textio import ReadFromCsv
from apache_beam.options.pipeline_options import PipelineOptions


class CsvOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--file_path",
            required=True,
            help="Csv file path"
        )


def run():
    """
    This pipeline shows how to read from Csv file.
    """

    options = CsvOptions()

    with beam.Pipeline(options=options) as p:

        output = (
            p
            | "Read from Csv file" >> ReadFromCsv(
                path=options.file_path
            )
            | "Log Data" >> Map(logging.info)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
