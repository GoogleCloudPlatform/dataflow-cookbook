#  Copyright 2024 Google LLC
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
import json
import logging
import typing

# third party libraries
import apache_beam as beam
from apache_beam import DoFn, Filter, Map, ParDo
from apache_beam.io.kafka import WriteToKafka
from apache_beam.io.textio import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

PROCESSED_TAG = "processed"
UNPROCESSED_TAG = "unprocessed"
SEVERITY = "severity"
PROTO_PAYLOAD = "protoPayload"


class KafkaOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--bootstrap_servers",
            required=True,
            help="Apache Kafka bootstrap server"
        )
        parser.add_argument(
            "--output_topic",
            required=True,
            help="Output Apache Kafka topic"
        )
        parser.add_argument(
            "--dlq_topic",
            required=True,
            help="Dead Letter Queue Apache Kafka topic"
        )
        parser.add_argument(
            "--input_file_path",
            required=True,
            help="The file path to read logs from."
        )


def run():
    """
    This Apache Beam pipeline processes log messages from a Google Cloud
    Storage files. The expected data format follows the standard Google
    Cloud log format, which can be achieved by routing logs to a Google Cloud
    Storage bucket via https://console.cloud.google.com/logs/router.

    It performs the following steps:
    1. Input Configuration:
        - Reads messages from the specified input Google Cloud Storage file.

    2. Message Splitting:
        - Divides messages into two categories:
        a. PROCESSED (contain both SEVERITY, PROTO_PAYLOAD fields)
        b. UNPROCESSED (missing one or both of these fields).

    4. Severity Filtering:
        - For PROCESSED messages, filters out those with severity other than
        "ERROR".

    5. Data Transformation:
        - Extracts timestamp and substantial content from the
        PROTO_PAYLOAD field for PROCESSED messages.

    6. Output Handling:
        - Writes transformed PROCESSED messages to a specified output Apache
         Kafka topic.
        - Sends UNPROCESSED messages to a Dead Letter Queue (DLQ) Apache
         Kafka topic.
    """
    options = KafkaOptions()

    with beam.Pipeline(options=options) as p:
        split_result = (
            p
            | "Read from file" >> ReadFromText(options.input_file_path)
            | "Split Messages"
            >> ParDo(SplitMessages()).with_outputs(
                UNPROCESSED_TAG, PROCESSED_TAG
            )
        )

        # Filter processed messages and write to output table
        (
            split_result[PROCESSED_TAG]
            | "Filter by Severity" >> Filter(filter_by_severity)
            | "Map to row for output"
            >> Map(to_kv_for_output).with_output_types(
                typing.Tuple[str, str]
            )
            | "Write to Kafka" >> WriteToKafka(
                producer_config={
                    "bootstrap.servers": options.bootstrap_servers
                },
                topic=options.output_topic,
                key_serializer=("org.apache.kafka.common.serialization."
                                "StringSerializer"),
                value_serializer=("org.apache.kafka.common.serialization."
                                  "StringSerializer")
            )
        )

        # Write unprocessed messages to DLQ
        (
            split_result[UNPROCESSED_TAG]
            | "Map to row for DLQ"
            >> Map(to_kv_for_dlq).with_output_types(
                typing.Tuple[str, str]
            )
            | "Write to DLQ" >> WriteToKafka(
                producer_config={
                    "bootstrap.servers": options.bootstrap_servers
                },
                topic=options.dlq_topic,
                key_serializer=("org.apache.kafka.common.serialization."
                                "StringSerializer"),
                value_serializer=("org.apache.kafka.common.serialization."
                                  "StringSerializer")
            )
        )


class SplitMessages(DoFn):
    def process(self, element):
        # third party libraries
        from apache_beam.pvalue import TaggedOutput

        if (SEVERITY in element) & (PROTO_PAYLOAD in element):
            yield TaggedOutput(PROCESSED_TAG, json.loads(element))
        else:
            yield TaggedOutput(UNPROCESSED_TAG, json.loads(element))


def filter_by_severity(log):
    # Filter logs by severity level (only process logs with severity "ERROR")
    return log[SEVERITY].upper() == "ERROR"


def to_kv_for_dlq(log):

    kv = (log["timestamp"], json.dumps(log.get("resource")))
    return kv


def to_kv_for_output(log):

    # Example transformation: Extract relevant information from the log
    payload = log[PROTO_PAYLOAD]
    message = (
        f"Error occurred with resource {payload['resourceName']} "
        f"[{payload['methodName']}]"
    )
    kv = (log["timestamp"], message)
    return kv


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
