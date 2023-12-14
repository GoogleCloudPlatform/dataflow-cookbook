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
from apache_beam.io.textio import WriteToJson
from apache_beam.options.pipeline_options import PipelineOptions


class JsonOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--file_path",
            default="gs://your-bucket/your-file.json",
            help="Json file path"
        )


def run():
    """
    This pipeline shows how to write to Json file.
    """

    options = JsonOptions()

    with beam.Pipeline(options=options) as p:

        elements = [
            (1, "Charles"),
            (2, "Alice"),
            (3, "Bob"),
            (4, "Amanda"),
            (5, "Alex"),
            (6, "Eliza")
        ]

        output = (
            p
            | "Create" >> Create(elements)
            | "Map to Row" >> Map(map_to_row)
            | "Write to Json file" >> WriteToJson(
                path=options.file_path
            )
        )


def map_to_row(element):
    """
    Converts a given element into a Beam Row object.
    """
    return beam.Row(id=element[0], name=element[1])


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
