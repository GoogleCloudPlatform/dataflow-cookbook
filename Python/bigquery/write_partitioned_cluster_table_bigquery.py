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
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


def make_row(element):
  row_fields = element.split(", ")
  return {
      "name": row_fields[0],
      "date": row_fields[1],
      "country": row_fields[2]
  }


def run(argv=None):
  elements = [
      "Charles, 2021-02-01, USA",
      "Alice, 2021-02-04, Spain",
      "Bob, 2021-02-02, USA",
      "Amanda, 2021-02-07, France",
      "Alex, 2021-02-06, Mexico",
      "Eliza, 2021-02-02, Japan"
  ]

  class WriteBigQueryOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          "--output_table",
          help="BQ Table to write")

  table_schema = "name:STRING, date:DATE, country:STRING"
  options = WriteBigQueryOptions()
  with beam.Pipeline(options=options) as p:
    output = (p | Create(elements)
                | Map(make_row)
                | WriteToBigQuery(options.output_table,
                                  schema=table_schema,
                                  write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                                  create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                  additional_bq_parameters={
                                      "timePartitioning": {"type": "DAY"},
                                      "clustering": {"fields": ["country"]}}))

if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()