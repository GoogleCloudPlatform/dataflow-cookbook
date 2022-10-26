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
from apache_beam.io.avroio import WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):

  class WriteAvroOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--output",
            help="GCS path to write")

  options = WriteAvroOptions()

  schema = {
    "namespace": "example.avro",
    "type": "record",
    "name": "People",
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "age", "type": "long"},
      {"name": "job", "type": "string"}
    ]
  }


  elements = [
      {
          "name": "Maria",
          "age": 19,
          "job": "CEO"
      },
      {
          "name": "Sara",
          "age": 44,
          "job": "Medic"
      },
      {
          "name": "Juan",
          "age": 31,
          "job": "Data Engineer"
      },
      {
          "name": "Kim",
          "age": 25,
          "job": "Lawyer"
      },
      {
          "name": "Roger",
          "age": 99,
          "job": "Pipeline Fixer"
      },
  ]

  with beam.Pipeline(options=options) as p:

    (p | Create(elements)
       # Input has to be a dict
       | WriteToAvro(
            options.output,
            schema=schema,
        )
    )


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()