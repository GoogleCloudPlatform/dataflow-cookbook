#  Copyright 2023 Google LLC
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
from apache_beam import Create
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

USERS_TABLE = "users"
COUNTRIES_TABLE = "countries"


class BigQueryOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--users_table",
            default="your-users-table",
            help="Google Cloud BigQuery table to store users"
        )
        parser.add_argument(
            "--countries_table",
            default="your-countries-table",
            help="Google Cloud BigQuery table to store countries"
        )


def run():

    options = BigQueryOptions()

    with beam.Pipeline(options=options) as p:

        elements = [
            {'name': 'Charles', 'year': 1995, 'country_code': 'MX'},
            {'country_code': 'MX', 'country_name': 'Mexico'},
            {'name': 'Alice', 'year': 1997, 'country_code': 'FR'},
            {'country_code': 'FR', 'country_name': 'France'},
            {'name': 'Alex', 'year': 1999, 'country_code': 'CA'},
            {'country_code': 'CA', 'country_name': 'Canada'},
        ]
        # define the BigQuery table schemas
        users_schema = "name:STRING, year:INTEGER, country_code:STRING"
        countries_schema = "country_code:STRING, country_name:STRING"

        schema_map = beam.pvalue.AsDict(
            p
            | "MakeSchemas"
            >> beam.Create(
                [(options.users_table, users_schema),
                 (options.countries_table, countries_schema)]
            )
        )

        table_map = beam.pvalue.AsDict(
            p
            | "MakeTables"
            >> beam.Create(
                [(USERS_TABLE, options.users_table),
                 (COUNTRIES_TABLE, options.countries_table)]
            )
        )

        # Pass schema_map and table_map as side inputs
        output = (
            p
            | Create(elements)
            | WriteToBigQuery(
                table=lambda row, tables:
                (tables[USERS_TABLE] if 'year' in row
                 else tables[COUNTRIES_TABLE]),
                schema=lambda table, schemas:
                schemas.get(table, None),
                table_side_inputs=(table_map, ),
                schema_side_inputs=(schema_map, ),
                method='STREAMING_INSERTS'
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
