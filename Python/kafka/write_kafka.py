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
from apache_beam.io.kafka import WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions


class KafkaOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--bootstrap_servers",
            default="localhost:9092",
            help="Apache Kafka bootstrap server"
        )
        parser.add_argument(
            "--topic",
            default="your-topic",
            help="Apache Kafka topic"
        )


def run():
    """
    This pipeline shows how to write to Apache Kafka.
    """

    options = KafkaOptions()

    with beam.Pipeline(options=options) as p:

        elements = [
            (1, "Charles"),
            (2, "Alice"),
            (3, "Bob"),
            (4, "Amanda"),
            (5, "Alex"),
            (6, "Eliza")
        ]

        output = (
            p
            | "Create" >> Create(elements)
            | "Write to Kafka" >> WriteToKafka(
                producer_config={
                    "bootstrap.servers": options.bootstrap_servers
                },
                topic=options.topic,
                key_serializer=
                    "org.apache.kafka.common.serialization.LongSerializer",
                value_serializer=
                    "org.apache.kafka.common.serialization.StringSerializer"
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
