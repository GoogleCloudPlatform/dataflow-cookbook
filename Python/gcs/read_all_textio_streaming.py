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
from apache_beam import Map, WindowInto
from apache_beam.io import ReadFromPubSub
from apache_beam.io.textio import ReadAllFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
from apache_beam.transforms.combiners import Count


def run(argv=None):
    class ReadAllStreamingTextOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                "--topic",
                description="Provide a topic to read from",
                dest="topic",
                help="You need to create this topic and the messages will be GCS paths.",
            )

    options = ReadAllStreamingTextOptions(streaming=True)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read GCS paths" >> ReadFromPubSub(topic=options.topic)
            | "Decode messages" >> Map(lambda x: x.decode("utf-8"))
            | "Read File from GCS" >> ReadAllFromText()
            | WindowInto(window.FixedWindows(300))  # 5 * 60sec = 5min
            | "Count Files" >> Count.Globally().without_defaults()
            | "Log" >> Map(lambda x: logging.info("Total lines %d", x))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
