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
from apache_beam.io.textio import ReadAllFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import Count


def run(argv=None):

  # Define the text file GCS locations.
  # You should replace these locations with the paths to your text files.
  elements = ["gs://apache-beam-samples/shakespeare/kinglear.txt",
              "gs://apache-beam-samples/shakespeare/macbeth.txt",
              "gs://apache-beam-samples/shakespeare/a*"]

  options = PipelineOptions()

  # ReadAllFromText reads the files from elements
  #  and parse each file as newline-delimited elements
  # At last, Count. globally() counts the total number of lines for all files
  with beam.Pipeline(options=options) as p:

    (p | Create(elements)
       | "Read File from GCS" >> ReadAllFromText()
       | Count.Globally()
       | "Log" >> Map(lambda x: logging.info("Total lines %d", x)))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
