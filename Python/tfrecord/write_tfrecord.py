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
from apache_beam.io.tfrecordio import WriteToTFRecord
from apache_beam.options.pipeline_options import PipelineOptions


class TFRecordOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--file_path_prefix",
            default="your-file-path-prefix",
            help="A file path prefix to write TFRecords files to."
        )


def run():
    """
    This pipeline shows how to write to TFRecord format.
    """

    options = TFRecordOptions()

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
            | "Map to Bytes" >> Map(map_to_bytes)
            | "Write to TFRecord" >> WriteToTFRecord(
                file_path_prefix=options.file_path_prefix
            )
        )


def map_to_bytes(element):
    """
    Serializes the input element using pickle library and
    returns the bytes representation.
    By default TFRecordIO transforms use `coders.BytesCoder()`.
    """
    # third party libraries
    import pickle

    return pickle.dumps(element)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
