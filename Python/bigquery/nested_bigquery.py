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
from apache_beam import Map
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


def nested_fn(row):
  new_row = {}
  new_row["type"] = row["type"]
  if row["repository"]:
    new_row["repository"] = {}
    new_row["repository"]["description"] = row["repository"]["description"]
    new_row["repository"]["url"] = row["repository"]["url"]

  return new_row


class NestedBigQueryOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
      "--output_table",
      help="BQ Table to write")

def run(argv=None):
  """This pipeline shows how to read, write and modify nested fields from BigQuery"""

  schema = {
    "fields": [
      {
      "name": "type",
      "type": "STRING",
      "mode": "NULLABLE"
      },
      {
      "name": "repository",
      "type": "RECORD",
      "mode": "NULLABLE",
      "fields": [
        {
        "name": "url",
        "type": "STRING",
        "mode": "NULLABLE"
        },
        {
        "name": "description",
        "type": "STRING",
        "mode": "NULLABLE"
        }
      ]
    }]
  }
  sql = ("SELECT repository, type FROM "
         "`bigquery-public-data.samples.github_nested` LIMIT 100")
  options = NestedBigQueryOptions()

  with beam.Pipeline(options=options) as p:
    (p | ReadFromBigQuery(query=sql, use_standard_sql=True)
       | Map(nested_fn)
       | WriteToBigQuery(
          options.output_table,
          schema=schema,
          write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
          create_disposition=BigQueryDisposition.CREATE_IF_NEEDED))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
