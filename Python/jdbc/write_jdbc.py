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
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import apache_beam as beam
import typing

from apache_beam import coders
from apache_beam import Create
from apache_beam import Map
from apache_beam.io.jdbc import WriteToJdbc
from apache_beam.options.pipeline_options import PipelineOptions
from typing import NamedTuple


class ExampleRow(NamedTuple):
    id: int
    name: str


class JdbcOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--table_name',
            default='your-table-name',
            help='Table name'
        )
        parser.add_argument(
            '--jdbc_url',
            default='jdbc:postgresql://localhost:5432/template1',
            help='JDBC URL'
        )
        parser.add_argument(
            '--driver_class_name',
            default='org.postgresql.Driver',
            help='Driver class name'
        )
        parser.add_argument(
            '--username',
            default='postgres',
            help='Username'
        )
        parser.add_argument(
            '--password',
            default='postgres',
            help='Password'
        )


def run():
    """
    This pipeline shows how to write to JDBC.
    """

    options = JdbcOptions()
    coders.registry.register_coder(ExampleRow, coders.RowCoder)

    with beam.Pipeline(options=options) as p:

        elements = [
            (1, "Charles"),
            (2, "Alice"),
            (3, "Bob"),
            (4, "Amanda"),
            (5, "Alex"),
            (6, "Eliza")
        ]

        output = (p | 'Create' >> Create(elements)
                    | 'Map to ExampleRow' >> Map(make_jdbc_row)
                        .with_output_types(ExampleRow)
                    | 'Write to jdbc' >> WriteToJdbc(
                      table_name=options.table_name,
                      driver_class_name=options.driver_class_name,
                      jdbc_url=options.jdbc_url,
                      username=options.username,
                      password=options.password
                    ))


def make_jdbc_row(element):

    return ExampleRow(element[0], element[1])


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
