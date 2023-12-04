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
  """Parse element to create a row using Dict
  """
  row_fields = element.split(", ")
  return {
      "name": row_fields[0],
      "year": int(row_fields[1]),
      "country": row_fields[2]
  }


def table_fn(element, base_table):
  """Select the table name based on the input data
  """
  if element["year"] > 1997:
    return base_table + "after"
  else:
    return base_table + "before"


def run(argv=None):
  elements = [
      "Charles, 1995, USA",
      "Alice, 1997, Spain",
      "Bob, 1995, USA",
      "Amanda, 1991, France",
      "Alex, 1999, Mexico",
      "Eliza, 2000, Japan"
  ]

  class WriteBigQueryOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          "--output_table",
          required=True,
          help="BQ Table destination")

  # the BigQuery table schema
  table_schema = "name:STRING, year:INTEGER, country:STRING"

  options = WriteBigQueryOptions()
  base_table = options.output_table

  # run the pipeline to write data into different BigQuery table
  with beam.Pipeline(options=options) as p:
    output = (p | Create(elements)
                | Map(make_row)
                | WriteToBigQuery(table=lambda row: table_fn(row, base_table),
                                  schema=table_schema,
                                  write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                                  create_disposition=BigQueryDisposition.CREATE_IF_NEEDED))

if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()