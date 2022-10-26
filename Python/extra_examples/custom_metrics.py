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
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions


class MyMetricDoFn(DoFn):

  def __init__(self):
    self.counter_enroute = Metrics.counter("Message Key", "Enroute")
    self.counter_dropoff = Metrics.counter("Message Key", "Dropoff")
    self.counter_pickup = Metrics.counter("Message Key", "Pickup")
    self.counter_people_in_taxi = Metrics.counter("People", "Delta people in taxi")
    self.distribution_cost = Metrics.distribution("People", "Cost (x1000)")

  def process(self, element):
    status = element["ride_status"]
    if status == "dropoff":
      self.counter_dropoff.inc()
      self.counter_people_in_taxi.dec()

      cost = element["meter_reading"]
      self.distribution_cost.update(cost * 1000)  # Multiplying by 1000 gives more significant decimals
    elif status == "enroute":
      self.counter_enroute.inc()
    elif status == "pickup":
      self.counter_pickup.inc()
      self.counter_people_in_taxi.inc()
    return element


def run(argv=None):
  topic = "projects/pubsub-public-data/topics/taxirides-realtime"
  options = PipelineOptions(save_main_session=True, streaming=True)
  with beam.Pipeline(options=options) as p:
    output = (p
              | "ReadFromPubSub" >> ReadFromPubSub(topic=topic)
              | "Load Json" >> Map(json.loads)
              | "MyMetricDoFn()" >> ParDo(MyMetricDoFn()))

if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
