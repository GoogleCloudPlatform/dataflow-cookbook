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
import re

import apache_beam as beam
from apache_beam import CombinePerKey
from apache_beam import Create
from apache_beam import FlatMap
from apache_beam import Map
from apache_beam import PTransform
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
  options = PipelineOptions()

  class WordCount(PTransform):
    def expand(self, pcoll):
      count = (pcoll | "Split" >> FlatMap(self._split_words)
                     | "Count" >> CombinePerKey(sum)
               )
      return count

    def _split_words(self, text):
        words = re.findall(r'[\w\']+', text.strip(), re.UNICODE)
        return [(x, 1) for x in words]


  with beam.Pipeline(options=options) as p:

    elements = [
        "From fairest creatures we desire increase",
        "That thereby beauty’s rose might never die",
        "But as the riper should by time decease",
        "His tender heir might bear his memory",
        "But thou, contracted to thine own bright eyes",
        "Feed’st thy light’st flame with self-substantial fuel",
        "Making a famine where abundance lies",
        "Thyself thy foe, to thy sweet self too cruel",
        "Thou that art now the world’s fresh ornament",
        "And only herald to the gaudy spring",
        "Within thine own bud buriest thy content",
        "And, tender churl, makest waste in niggarding",
        "Pity the world, or else this glutton be",
        "To eat the world’s due, by the grave and thee"
    ]

    (p | Create(elements)
       | WordCount()
       | Map(logging.info)
     )


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()