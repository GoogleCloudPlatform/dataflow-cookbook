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


def repeated_fn(row):
    new_row = {}
    new_row["term"] = row["term"]
    new_row["year"] = []
    for years in row["years"]:
        if years["year"] >= 1900:
            new_row["year"].append(years["year"])
    return new_row

class RepeatedBigQueryOptions(PipelineOptions):

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
              "name": "term",
              "type": "STRING",
              "mode": "NULLABLE"
          },
          {
              "name": "year",
              "type": "INTEGER",
              "mode": "REPEATED"
          }
      ]
  }
  sql = ("SELECT term, years FROM "
         "`bigquery-public-data.google_books_ngrams_2020.eng_fiction_1`")
  options = RepeatedBigQueryOptions()
  with beam.Pipeline(options=options) as p:
    (p | ReadFromBigQuery(query=sql, use_standard_sql=True)
       | Map(repeated_fn)
       | WriteToBigQuery(
            options.output_table,
            schema=schema,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
