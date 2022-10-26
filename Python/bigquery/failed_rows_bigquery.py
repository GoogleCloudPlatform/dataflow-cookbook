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

import json
import logging

import apache_beam as beam
from apache_beam import Filter
from apache_beam import Map
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.io.gcp.bigquery import BigQueryWriteFn
from apache_beam.io.gcp.bigquery import RetryStrategy
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
  """This pipeline shows how to access the rows that failed being inserted to BigQuery"""
  topic = "projects/pubsub-public-data/topics/taxirides-realtime"

  class FailedRowsOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          "--output_table",
          help="BQ Table to write")

  # Schema is wrong so that rows fail
  wrong_schema = "test:STRING"
  options = FailedRowsOptions()
  with beam.Pipeline(options=options) as p:
    bq_insert = (p | "ReadFromPubSub" >> ReadFromPubSub(topic=topic)
                   | "Load Json" >> Map(json.loads)
                   | Filter(lambda r: r["ride_status"] == "dropoff")
                   # Since the schema is wrong, the rows will fail
                   | WriteToBigQuery(options.output_table,
                                  schema=wrong_schema,
                                  insert_retry_strategy=RetryStrategy.RETRY_NEVER,
                                  create_disposition=BigQueryDisposition.CREATE_IF_NEEDED))

    (bq_insert[BigQueryWriteFn.FAILED_ROWS]
                | "Failed" >> Map(lambda x: logging.info("Failed row: %s", x))
    )

if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
