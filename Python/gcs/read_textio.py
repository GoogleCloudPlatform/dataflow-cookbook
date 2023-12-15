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
from apache_beam.io.textio import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import Count


def run(argv=None):
    class ReadTextOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            # Add a command line flag to be parsed along
            # with other normal PipelineOptions
            parser.add_argument(
                "--path",
                default="gs://dataflow-samples/shakespeare/kinglear.txt",
                help="GCS path to read",
            )

    options = ReadTextOptions()

    # Create a Beam pipeline with 3 steps:
    # 1) Read text. This will emit one record per line
    # 2) Count.Globally(). This will count the number of
    # elements in the PCollection.
    # 3) Log the output.
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadText" >> ReadFromText(options.path)
            | Count.Globally()
            | "Log" >> Map(lambda x: logging.info("Total lines %d", x))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
