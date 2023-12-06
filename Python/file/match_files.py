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
from apache_beam.io.fileio import MatchFiles
from apache_beam.options.pipeline_options import PipelineOptions


class FilesOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--file_pattern",
            default="your-file-pattern",
            help="File pattern"
        )


def run():
    """
    This pipeline shows how to read matching files in the form of ``FileMetadata`` objects.
    """

    options = FilesOptions()

    with beam.Pipeline(options=options) as p:

        output = (p | "Match files" >> MatchFiles(
                        file_pattern=options.file_pattern
                    )
                    | "Log Data" >> Map(logging.info))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
