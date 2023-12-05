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
from apache_beam.io import ReadAllFromBigQuery
from apache_beam.io import ReadFromBigQueryRequest
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
  """Use ReadAllFromBigQuery to define table and query reads from
  BigQuery at pipeline runtime.
  """
  query1 = ("SELECT repository_language, COUNT(repository_language) totalRepos"
            " FROM `bigquery-public-data.samples.github_timeline` GROUP BY 1");
  query2 = ("SELECT year, mother_residence_state, COUNT(*) countByYearState"
            " FROM `bigquery-public-data.samples.natality` GROUP BY 1, 2")
  table = "bigquery-public-data:samples.shakespeare"

  options = PipelineOptions()
  with beam.Pipeline(options=options) as p:
    read_requests = p | Create([
      ReadFromBigQueryRequest(query=query1),
      ReadFromBigQueryRequest(query=query2),
      ReadFromBigQueryRequest(table=table)
    ])
    output = (read_requests | "ReadFromAll" >> ReadAllFromBigQuery()
                            | "LogMessages" >> Map(logging.info)
              )

if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
