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

# standard libraries
import logging

# third party libraries
import apache_beam as beam
from apache_beam import Map
from apache_beam.io import ReadFromBigQuery
from apache_beam.io.gcp.internal.clients import bigquery


def run(argv=None):
    # Configure the table we are reading from.
    table = bigquery.TableReference(
        projectId="bigquery-public-data",
        datasetId="samples",
        tableId="github_timeline",
    )

    # Create a Beam pipeline with 2 steps: read from BigQuery and log the data.
    with beam.Pipeline() as p:
        output = (
            p
            | "ReadTable" >> ReadFromBigQuery(table=table)
            | "LogData" >> Map(lambda data: logging.info(data))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
