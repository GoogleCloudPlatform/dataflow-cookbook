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
from apache_beam import DoFn, Map, ParDo
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.userstate import (
    BagStateSpec,
    CombiningValueStateSpec,
    TimeDomain,
    TimerSpec,
    on_timer,
)


class TimerDoFn(beam.DoFn):
    """
    GroupIntoBatches implements a similar logic.
    """

    BUFFER_TIMER = TimerSpec("expiry", TimeDomain.REAL_TIME)

    BUFFER_BAG = BagStateSpec("rides", StrUtf8Coder())
    COUNT_STATE = CombiningValueStateSpec("count", combine_fn=sum)

    def __init__(self):
        self.status_timer = {"pickup": 60, "enroute": 10, "dropoff": 60}

    def process(
        self,
        element,
        count_state=beam.DoFn.StateParam(COUNT_STATE),
        ride_state=beam.DoFn.StateParam(BUFFER_BAG),
        timer=beam.DoFn.TimerParam(BUFFER_TIMER),
    ):
        # standard libraries
        import time

        ride_id = element[1]

        if count_state.read() == 0:
            # Start timer if no element already
            logging.info("Intializing timer for key %s", element[0])
            timer_length = self.status_timer[element[0]]
            timer.set(time.time() + timer_length)
            count_state.add(1)

        # Add ride id to bag
        ride_state.add(ride_id)

    @on_timer(BUFFER_TIMER)
    def timer_fn(
        self,
        ride_state=DoFn.StateParam(BUFFER_BAG),
        count_state=DoFn.StateParam(COUNT_STATE),
    ):
        logging.info("Releasing buffer")
        for ride in ride_state.read():
            yield ride

        # Clear states
        ride_state.clear()
        count_state.clear()


def run(argv=None):
    options = PipelineOptions(streaming=True)
    with beam.Pipeline(options=options) as p:
        topic = "projects/pubsub-public-data/topics/taxirides-realtime"

        pubsub = (
            p
            | "Read Topic" >> ReadFromPubSub(topic=topic)
            | "Json Loads" >> Map(json.loads)
            # SDFn need KVs as input. They are applied in a Key and Window basis
            | "KV" >> Map(lambda x: (x["ride_status"], x["ride_id"]))
        )

        (
            pubsub
            | "TimerDoFn" >> ParDo(TimerDoFn())
            | "Pass" >> Map(lambda x: x)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
