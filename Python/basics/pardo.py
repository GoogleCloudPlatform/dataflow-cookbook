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


# ParDo allows 1 to Many outputs while Map only allows 1 to 1 outputs.
class SplitFn(DoFn):

  def process(self, element):
    return element.split()


def run(argv=None):
  elements = [
      "Lorem ipsum dolor sit amet. Consectetur adipiscing elit",
      "Sed eu velit nec sem vulputate loborti",
      "In lobortis augue vitae sagittis molestie. Mauris volutpat tortor non ",
      "Ut blandit massa et risus sollicitudin auctor"]

  with beam.Pipeline() as p:
    output = (p | Create(elements)
                | "Split Output" >> ParDo(SplitFn())
                | "Log"
 >> Map(logging.info))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
