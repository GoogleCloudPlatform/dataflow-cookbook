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

# standard libraries
import logging
import typing
from typing import NamedTuple

# third party libraries
import apache_beam as beam
from apache_beam import coders
from apache_beam import Map
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.options.pipeline_options import PipelineOptions


class ExampleRow(NamedTuple):
    id: int
    name: str


class JdbcOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--table_name",
            default="your-table-name",
            help="Table name"
        )
        parser.add_argument(
            "--jdbc_url",
            default="jdbc:postgresql://localhost:5432/template1",
            help="JDBC URL"
        )
        parser.add_argument(
            "--driver_class_name",
            default="org.postgresql.Driver",
            help="Driver class name"
        )
        parser.add_argument(
            "--username",
            default="postgres",
            help="Username"
        )
        parser.add_argument(
            "--password",
            default="postgres",
            help="Password"
        )


def run():
    """
    This pipeline shows how to read from JDBC.
    """

    options = JdbcOptions()
    coders.registry.register_coder(ExampleRow, coders.RowCoder)

    with beam.Pipeline(options=options) as p:

        output = (
            p
            | "Read from jdbc" >> ReadFromJdbc(
                table_name=options.table_name,
                driver_class_name=options.driver_class_name,
                jdbc_url=options.jdbc_url,
                username=options.username,
                password=options.password
            )
            | "Map to ExampleRow" >> Map(
                lambda element: ExampleRow(element[0], element[1])
            )
            | "Log Data" >> Map(logging.info)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
