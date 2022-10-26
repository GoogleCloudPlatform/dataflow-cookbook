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
from apache_beam import Map
from apache_beam import pvalue


def converter(elements, rates):
  currency, value = elements
  converted_value = value * rates[currency]
  return converted_value


def run(argv=None):
  elements = [
      ("USD", 3.1415),
      ("USD", 1729.0),
      ("CHF", 2.7182),
      ("EUR", 1.618),
      ("CHF", 1.1),
      ("CHF", 342.45),
      ("EUR", 890.01)
  ]
  side_elements = [
      ("USD", 1.0),
      ("EUR", 0.8),
      ("CHF", 0.9)
  ]

  with beam.Pipeline() as p:
    side_input = p | "side input" >> Create(side_elements)
    output = (p | Create(elements)
                | "Convert" >> Map(converter, rates=pvalue.AsDict(side_input))
                | "Log"
 >> Map(logging.info))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
