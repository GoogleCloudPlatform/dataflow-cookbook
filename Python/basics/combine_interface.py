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
from apache_beam import CombineFn
from apache_beam import CombineGlobally
from apache_beam import Create
from apache_beam import Map


class AverageFn(CombineFn):
  """
  This is the same as the Mean PTransform, but it's used as an example
  to show the combiner interface
  """
  def create_accumulator(self):
    # create and initialise accumulator for sum and count
    initial_sum = 0
    initial_count = 0
    return initial_sum, initial_count

  def add_input(self, accumulator, element):
    # accumulates each element from input in accumulator
    new_total = accumulator[0] + element
    new_count = accumulator[1] + 1
    return new_total, new_count

  def merge_accumulators(self, accumulators):
    # Multiple accumulators could be processed in parallel,
    # this function merges them
    sums = [accumulator[0] for accumulator in accumulators]
    counts = [accumulator[1] for accumulator in accumulators]
    return sum(sums), sum(counts)

  def extract_output(self, accumulator):
    # calculations before final output
    return accumulator[0] / accumulator[1]


def run(args=None):
  elements = range(100)

  with beam.Pipeline() as p:
    output = (p | Create(elements)
                | "Global Average" >> CombineGlobally(AverageFn())
                | "Log" >> Map(logging.info))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
