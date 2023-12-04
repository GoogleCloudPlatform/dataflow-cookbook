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

import logging
import apache_beam as beam

from apache_beam import Map
from apache_beam.io.tfrecordio import ReadFromTFRecord
from apache_beam.options.pipeline_options import PipelineOptions


class TFRecordOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--file_pattern',
            default="your-file-pattern",
            help='A file glob pattern to read TFRecords from.'
        )


def run():
    """
    This pipeline shows how to read from TFRecord format.
    """

    options = TFRecordOptions()

    with beam.Pipeline(options=options) as p:

        output = (p | "Read from TFRecord" >> ReadFromTFRecord(
                        file_pattern=options.file_pattern
                    )
                    | "Map from bytes" >> Map(map_from_bytes)
                    | "Log Data" >> Map(logging.info))


def map_from_bytes(element):
    import pickle

    return pickle.loads(element)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
