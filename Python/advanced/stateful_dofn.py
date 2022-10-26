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

import json
import logging

import apache_beam as beam
from apache_beam import DoFn
from apache_beam import Map
from apache_beam import ParDo
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import CombiningValueStateSpec


def run(argv=None):
  class StatefulDoFn(DoFn):
    """
    GroupIntoBatches implements a similar logic.
    When using StatefulDoFns be careful of not keeping the state forever and clearing state. This example is OK
    because we know the keys are always incoming, but if we have  sparse keys, we may keep the buffer up forever
    (e.g., we trigger every 100 elements but we only got 99 for that key. See `timer_dofn` for an example that
    would fix that)
    """
    BUFFER_RIDES = BagStateSpec('rides', StrUtf8Coder())
    COUNT_STATE = CombiningValueStateSpec('count', combine_fn=sum)

    def __init__(self):
      self.status_max_bag = {
        "pickup": 100,
        "enroute": 10000,
        "dropoff": 100
      }

    def process(self,
                element,
                count_state=DoFn.StateParam(COUNT_STATE),
                ride_state=DoFn.StateParam(BUFFER_RIDES)):

      ride_id = element[1]
      ride_status = element[0]

      # Add ride id to bag
      ride_state.add(ride_id)

      # Increase counter
      count_state.add(1)
      count = count_state.read()

      max_size = self.status_max_bag[ride_status]

      # If counter is over max bag size, release buffer
      if count > max_size:
        logging.info("Releasing buffer for key %s", element[0])
        for ride in ride_state.read():
          yield ride

        # Clear states
        ride_state.clear()
        count_state.clear()

  options = PipelineOptions(streaming=True)
  with beam.Pipeline(options=options) as p:
    topic = "projects/pubsub-public-data/topics/taxirides-realtime"

    pubsub = (p | "Read Topic" >> ReadFromPubSub(topic=topic)
                | "Json Loads" >> Map(json.loads)
                # SDFn need KVs as input. They are applied in a Key and Window basis
                | "KV" >> Map(lambda x: (x["ride_status"], x["ride_id"]))
              )

    (pubsub | "StatefulDoFn" >> ParDo(StatefulDoFn())
            | "Pass" >> Map(lambda x: x))

if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
