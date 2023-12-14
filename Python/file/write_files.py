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
from apache_beam.io.fileio import WriteToFiles
from apache_beam.options.pipeline_options import PipelineOptions


class FilesOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--path",
            default="your-path",
            help="The directory to write files into."
        )


def run():
    """
    This pipeline shows how to write write to a set of output files.
    """

    options = FilesOptions()

    with beam.Pipeline(options=options) as p:
        elements = [
            {'id': 1, 'name': 'Charles'},
            {'id': 2, 'name': 'Alice'},
            {'id': 3, 'name': 'Bob'},
            {'id': 4, 'name': 'Amanda'},
            {'id': 5, 'name': 'Alex'},
            {'id': 6, 'name': 'Eliza'}
        ]

        output = (
            p
            | "Create" >> Create(elements)
            | "Serialize" >> Map(map_to_json)
            | "Write to files" >> WriteToFiles(
                path=options.path
            )
        )


def map_to_json(element):
    """
    Converts a given input element into a JSON string.
    """

    # third party libraries
    import json

    return json.dumps(element)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
