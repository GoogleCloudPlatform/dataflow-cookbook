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
import json
import logging

# third party libraries
import apache_beam as beam
from apache_beam import DoFn, Filter, Map, ParDo
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions

PROCESSED_TAG = "processed"
UNPROCESSED_TAG = "unprocessed"


class PubSubOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_topic",
            default="projects/your-project/topics/your-input-test",
            help="Input PubSub topic",
        )
        parser.add_argument(
            "--output_topic",
            default="projects/your-project/topics/your-output-test",
            help="Output PubSub topic",
        )
        parser.add_argument(
            "--dlq_topic",
            default="projects/your-project/topics/your-dlq-test",
            help="Dead Letter Queue PubSub topic",
        )


def run():
    """
    This Apache Beam pipeline processes log messages from a Google Cloud
    Pub/Sub topic. The expected data format follows the standard Google Cloud
    log format, which can be achieved by routing logs to a Pub/Sub topic via
    https://console.cloud.google.com/logs/router.

    It performs the following steps:
    1. Input Configuration:
        - Reads messages from the specified input Pub/Sub topic.

    2. Message Parsing:
        - Parses each message as a JSON dictionary.

    3. Message Splitting:
        - Divides messages into two categories:
        a. PROCESSED (contain both 'severity' and 'jsonPayload' fields)
        b. UNPROCESSED (missing one or both of these fields).

    4. Severity Filtering:
        - For PROCESSED messages, filters out those with severity other than
        "ERROR".

    5. Data Transformation:
        - Extracts timestamp and message content from the 'jsonPayload' field
        for PROCESSED messages.

    6. Output Handling:
        - Writes transformed PROCESSED messages to a specified output Pub/Sub
        topic.
        - Sends UNPROCESSED messages to a Dead Letter Queue (DLQ) topic.
    """

    options = PubSubOptions(streaming=True)

    with beam.Pipeline(options=options) as p:
        split_result = (
            p
            | "Read from PubSub" >> ReadFromPubSub(topic=options.input_topic)
            | "Parse JSON" >> Map(lambda msg: json.loads(msg))
            | "Split Messages"
            >> ParDo(SplitMessages()).with_outputs(
                UNPROCESSED_TAG, PROCESSED_TAG
            )
        )

        # Filter processed messages and write to output topic
        (
            split_result[PROCESSED_TAG]
            | "Filter by Severity" >> Filter(filter_by_severity)
            | "Map to PubsubMessage for output"
            >> Map(to_pubsub_message_for_output)
            | "Write to PubSub"
            >> WriteToPubSub(options.output_topic, with_attributes=True)
        )

        # Write unprocessed messages to DLQ
        (
            split_result[UNPROCESSED_TAG]
            | "Map to PubsubMessage for DLQ" >> Map(to_pubsub_message_for_dlq)
            | "Write to DLQ"
            >> WriteToPubSub(options.dlq_topic, with_attributes=True)
        )


class SplitMessages(DoFn):
    def process(self, element):
        # third party libraries
        from apache_beam.pvalue import TaggedOutput

        if ("severity" in element) & ("jsonPayload" in element):
            yield TaggedOutput(PROCESSED_TAG, element)
        else:
            yield TaggedOutput(UNPROCESSED_TAG, element)


def filter_by_severity(log):
    # Filter logs by severity level (only process logs with severity "ERROR")
    return log.get("severity").upper() == "ERROR"


def to_pubsub_message_for_dlq(msg):
    # third party libraries
    from apache_beam.io import PubsubMessage

    return PubsubMessage(data=bytes(json.dumps(msg), "utf-8"), attributes=None)


def to_pubsub_message_for_output(log):
    # third party libraries
    from apache_beam.io import PubsubMessage

    # Example transformation: Extract relevant information from the log
    transformed_data = {
        "timestamp": log.get("timestamp"),
        "message": log.get("jsonPayload").get("message"),
    }
    data = bytes(
        f"Error log message: {transformed_data['message']} "
        f"[{transformed_data['timestamp']}]",
        "utf-8",
    )
    return PubsubMessage(data=data, attributes=transformed_data)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
