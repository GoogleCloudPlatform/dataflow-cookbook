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
from apache_beam import Create, Map
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
    class WriteTextOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                "--output",
                description="Output to write to",
                dest="output",
                required=True,
                help="Output file to write results to.",
            )
            parser.add_argument(
                "--max_number",
                dest="max_number",
                default=100,
                help="Number of shards to process.",
            )

    options = WriteTextOptions()
    elements = range(options.max_number)

    with beam.Pipeline(options=options) as p:
        (
            p
            | Create(elements)
            | "Format String"
            >> Map(
                lambda x: f"This element number is {x}"
            )  # Changing number to string and
            | "Write Files" >> WriteToText(options.output)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
