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

# third party libraries
import apache_beam as beam
from apache_beam import Create, Map
from apache_beam.io.mongodbio import WriteToMongoDB
from apache_beam.options.pipeline_options import PipelineOptions


class MongoDBOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--uri",
            default="mongodb://localhost:27017",
            help="The MongoDB connection string following the URI format"
        )
        parser.add_argument(
            "--db_name",
            required=True,
            help="The MongoDB database name"
        )
        parser.add_argument(
            "--collection",
            required=True,
            help="The MongoDB collection name"
        )


def run():
    """
    This pipeline shows how to write to MongoDB.
    """

    options = MongoDBOptions()

    with beam.Pipeline(options=options) as p:

        elements = [
            {'id': 1, 'name': 'Charles'},
            {'id': 2, 'name': 'Alice'},
            {'id': 3, 'name': 'Bob'},
            {'id': 4, 'name': 'Amanda'},
            {'id': 5, 'name': 'Alex'},
            {'id': 6, 'name': 'Eliza'}
        ]

        output = (
            p
            | "Create" >> Create(elements)
            | "Write to MongoDB" >> WriteToMongoDB(
                uri=options.uri,
                db=options.db_name,
                coll=options.collection
            )
            | "Log Data" >> Map(logging.info)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
