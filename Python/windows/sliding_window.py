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
from apache_beam import CombinePerKey, DoFn, ParDo, Pipeline, WindowInto, window
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions

INPUT_TOPIC = "projects/pubsub-public-data/topics/taxirides-realtime"


class ParseMessages(DoFn):
    """
    The input messages are based on key-value pairs ('ride_status': 'passenger_count').
    Parse data 'ride_status' and 'passenger_count' from messages
    """

    def process(self, element):
        parsed = json.loads(element.decode("utf-8"))
        if parsed["ride_status"].lower() != "enroute":
            ride_status = parsed["ride_status"]
            passenger_count = parsed["passenger_count"]
            yield (ride_status, passenger_count)


class WriteOutputs(DoFn):
    """Log the outputs"""

    def process(self, element):
        ride = element[0]
        passengers = element[1]
        logging.info("A total passengers of %d have been %s", passengers, ride)


def run(argv=None):
    # Parsing arguments
    class SlidingWindowOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                "--input_topic",
                help='Input PubSub topic of the form "projects/<PROJECT>/topics/<ToPIC>."',
                default=INPUT_TOPIC,
            )

    options = SlidingWindowOptions(streaming=True)

    # Defining our pipeline and its steps
    with Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(topic=options.input_topic)
            | "ParseMessages" >> ParDo(ParseMessages())
            # Apply Sliding Window of length 30 seconds and new window every 5 seconds
            | "ApplySlidingWindow" >> WindowInto(window.SlidingWindows(30, 5))
            | "SumPerKey" >> CombinePerKey(sum)
            | "LogOutputs" >> ParDo(WriteOutputs())
        )


if __name__ == "__main__":
    run()
