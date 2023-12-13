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
from apache_beam import Create, DoFn, ParDo
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions


class WriteFileSystems(DoFn):
    def process(self, element):
        # Beam's built-in FileSystems module has built in support for many
        # different backing storage systems, we use this to write our element.
        # Each input element is formatted as a Tuple of the form
        # <destination file, data to write>
        writer = FileSystems.create(element[0])
        writer.write(bytes(element[1], encoding="utf8"))
        writer.close()


def run(argv=None):
    # TODO: add your bucket names
    bucket1 = ""
    bucket2 = ""
    elements = [
        ("gs://" + bucket1 + "/beam/dynamic.txt", "line"),
        ("gs://" + bucket2 + "/beam/dynamic.txt", "line"),
    ]
    options = PipelineOptions(save_main_session=True)
    with beam.Pipeline(options=options) as p:
        output = (
            p
            | Create(elements)
            | "Write FileSystems" >> ParDo(WriteFileSystems())
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
