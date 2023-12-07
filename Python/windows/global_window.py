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
from apache_beam.transforms import trigger

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
    class GlobalWindowOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                "--input_topic",
                help='Input PubSub topic of the form "projects/<PROJECT>/topics/<ToPIC>."',
                default=INPUT_TOPIC,
            )

    options = GlobalWindowOptions(streaming=True)

    # When using GlobalWindows be careful of not keeping the state forever and clearing state. In this example we
    # add a compound trigger so that we clean up state and don't let any window without triggering (e.g., we trigger
    # every 100 elements but we only got 99 for that key). Check Java's `testingWindows` folder for more info.
    compound_trigger = trigger.AfterAny(
        trigger.AfterCount(100), trigger.AfterProcessingTime(60)
    )

    # Defining our pipeline and its steps
    with Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(topic=options.input_topic)
            | "ParseMessages" >> ParDo(ParseMessages())
            # Apply a Global Window and trigger repeatedly firing after at least 100 elements or 60s from first element.
            | "ApplyGlobalWindow"
            >> WindowInto(
                window.GlobalWindows(),
                trigger=trigger.Repeatedly(compound_trigger),
                accumulation_mode=trigger.AccumulationMode.DISCARDING,
            )
            | "SumPerKey" >> CombinePerKey(sum)
            | "LogOutputs" >> ParDo(WriteOutputs())
        )


if __name__ == "__main__":
    run()
