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

import logging

import apache_beam as beam
from apache_beam import Map
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions


def run():
  class ReadPubSubOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          "--topic",
          # Run on Dataflow or authenticate to not get 403 PermissionDenied
          default="projects/pubsub-public-data/topics/taxirides-realtime",
          help="PubSub topic to read")

  options = ReadPubSubOptions(streaming=True)

  with beam.Pipeline(options=options) as p:
    # When reading from a topic, a new subscription is created.
    (p | "Read PubSub topic" >> ReadFromPubSub(topic=options.topic)
       | "Message" >> Map(lambda msg: f"PubSub message:\n{msg}\n")
       | Map(logging.info))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
