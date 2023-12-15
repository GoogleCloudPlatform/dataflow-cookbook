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
#  See the License for the spe

# standard libraries
import logging

# third party libraries
import apache_beam as beam
from apache_beam import Create, Map
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


def make_row(element):
    """Parse element to create a row using Dict"""
    row_fields = element.split(", ")
    return {
        "name": row_fields[0],
        "year": int(row_fields[1]),
        "country": row_fields[2],
    }


def run(argv=None):
    elements = [
        "Charles, 1995, USA",
        "Alice, 1997, Spain",
        "Bob, 1995, USA",
        "Amanda, 1991, France",
        "Alex, 1999, Mexico",
        "Eliza, 2000, Japan",
    ]

    class WriteBigQueryOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            # Add a command line flag to be parsed along
            # with other normal PipelineOptions
            parser.add_argument(
                "--output_table", required=True, help="BQ Table to write"
            )

    # define the BigQuery table schema
    table_schema = "name:STRING, year:INTEGER, country:STRING"
    options = WriteBigQueryOptions()

    # run the pipeline to write data into a BigQuery table
    with beam.Pipeline(options=options) as p:
        output = (
            p
            | Create(elements)
            | Map(make_row)
            | WriteToBigQuery(
                options.output_table,
                schema=table_schema,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
