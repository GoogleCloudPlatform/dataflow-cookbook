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
from apache_beam import CombinePerKey
from apache_beam import Create
from apache_beam import Map


def run(argv=None):
  elements = [
      ("Latin", "Lorem ipsum dolor sit amet. Consectetur adipiscing elit. Sed eu velit nec sem vulputate loborti"),
      ("Latin", "In lobortis augue vitae sagittis molestie. Mauris volutpat tortor non purus elementum"),
      ("English", "From fairest creatures we desire increase"),
      ("English", "That thereby beauty's rose might never die"),
      ("English", "But as the riper should by time decease"),
      ("Spanish", "En un lugar de la Mancha, de cuyo nombre no quiero acordarme, no ha mucho"),
      ("Spanish", "tiempo que vivía un hidalgo de los de lanza en astillero, adarga antigua")
  ]

  with beam.Pipeline() as p:
    output = (p | Create(elements)
                | "Join per key with full stop" >> CombinePerKey(lambda x: ". ".join(x))
                | "Log"
 >> Map(logging.info))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
