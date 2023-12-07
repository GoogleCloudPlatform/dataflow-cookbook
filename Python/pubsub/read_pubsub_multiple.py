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
from apache_beam.io import MultipleReadFromPubSub, PubSubSourceDescriptor
from apache_beam.options.pipeline_options import PipelineOptions


def run():
    class ReadPubSubOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                "--sources",
                required=True,
                help="PubSub topics or subscriptions, separated by a coma,"
                "e.g.: projects/a/topics/t1,projects/a/topics/t2.",
            )

    options = ReadPubSubOptions(streaming=True)
    sources = [PubSubSourceDescriptor(s) for s in options.sources.split(",")]

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read multiple PubSub sources" >> MultipleReadFromPubSub(sources)
            | "Message" >> Map(lambda msg: f"PubSub message:\n{msg}\n")
            | Map(logging.info)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
