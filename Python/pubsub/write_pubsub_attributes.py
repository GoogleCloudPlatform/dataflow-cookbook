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
import json
import logging

# third party libraries
import apache_beam as beam
from apache_beam import Map, ParDo
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions

input_topic = "projects/pubsub-public-data/topics/taxirides-realtime"


def to_pubsub_message(element):
    # third party libraries
    from apache_beam.io import PubsubMessage

    if element["ride_status"] == "dropoff":
        attributes = {}
        attributes["timestamp"] = element["timestamp"]
        # Attributes need to be string-string
        attributes["passenger_count"] = str(element["passenger_count"])
        # Data needs to be string
        data = bytes(f"Ride id is {element['ride_id']}", "utf-8")
        message = PubsubMessage(data=data, attributes=attributes)
        yield message


def run():
    class WritePubSubAttrOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                "--topic",
                required=True,
                help="PubSub topic with attributes to write to.",
            )

    options = WritePubSubAttrOptions(streaming=True)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read PubSub topic" >> ReadFromPubSub(topic=input_topic)
            | Map(json.loads)
            | "Message" >> ParDo(to_pubsub_message)
            | WriteToPubSub(topic=options.topic, with_attributes=True)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
