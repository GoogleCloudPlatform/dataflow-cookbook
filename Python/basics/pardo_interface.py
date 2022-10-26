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
from apache_beam import Create
from apache_beam import DoFn
from apache_beam import Map
from apache_beam import ParDo
from apache_beam import window


class DoFnMethods(DoFn):

  def __init__(self):
    logging.debug("__init__")
    self.window = window.GlobalWindow()
    self.list = []

  def setup(self):
    logging.debug("setup")

  def start_bundle(self):
    logging.debug("start_bundle")
    self.list = []

  def process(self, element, window=DoFn.WindowParam):
    self.list.append(element)
    yield f"* process: {element}"

  def finish_bundle(self):
    from apache_beam.utils.windowed_value import WindowedValue
    # yielded elements from finish_bundle have to be type WindowedValue.
    yield WindowedValue(
        value=f"* finish_bundle: {self.list}",
        timestamp=0,
        windows=[self.window],
    )

  def teardown(self):
    logging.debug("teardown")


def run(argv=None):
  n = 20

  with beam.Pipeline() as p:
    output = (p | Create(range(n))
                | "DoFn Methods" >> ParDo(DoFnMethods())
                | "Log" >> Map(logging.info))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
