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
from typing import NamedTuple

# third party libraries
import apache_beam as beam
from apache_beam import coders, DoFn, Filter, Map, ParDo
from apache_beam.io.gcp.spanner import SpannerInsert
from apache_beam.io.textio import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

PROCESSED_TAG = "processed"
UNPROCESSED_TAG = "unprocessed"
SEVERITY = "severity"
PROTO_PAYLOAD = "protoPayload"


class ExampleOutputRow(NamedTuple):
    message: str
    timestr: str


class ExampleDLQRow(NamedTuple):
    resource: str
    timestr: str


class SpannerOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along
        # with other normal PipelineOptions
        parser.add_argument(
            "--project_id",
            required=True,
            help="Google Cloud project ID"
        )
        parser.add_argument(
            "--instance_id",
            required=True,
            help="Google Cloud Spanner instance ID"
        )
        parser.add_argument(
            "--database_id",
            required=True,
            help="Google Cloud Spanner database ID"
        )
        parser.add_argument(
            "--output_table",
            required=True,
            help="Output Google Cloud Spanner table name"
        )
        parser.add_argument(
            "--dlq_table",
            required=True,
            help="Dead Letter Queue Google Cloud Spanner table name"
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
        - Writes transformed PROCESSED messages to a specified output Google
        Cloud Spanner table.
        - Sends UNPROCESSED messages to a Dead Letter Queue (DLQ) Google
        Cloud Spanner table.
    """
    options = SpannerOptions()
    coders.registry.register_coder(ExampleOutputRow, coders.RowCoder)
    coders.registry.register_coder(ExampleDLQRow, coders.RowCoder)

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
            >> Map(to_row_for_output).with_output_types(ExampleOutputRow)
            | "Write to Spanner" >> SpannerInsert(
                project_id=options.project_id,
                instance_id=options.instance_id,
                database_id=options.database_id,
                table=options.output_table
            )
        )

        # Write unprocessed messages to DLQ
        (
            split_result[UNPROCESSED_TAG]
            | "Map to row for DLQ"
            >> Map(to_row_for_dlq).with_output_types(ExampleDLQRow)
            | "Write to DLQ"
            >> SpannerInsert(
                project_id=options.project_id,
                instance_id=options.instance_id,
                database_id=options.database_id,
                table=options.dlq_table
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


def to_row_for_dlq(log):

    return ExampleDLQRow(json.dumps(log.get("resource")), log["timestamp"])


def to_row_for_output(log):

    # Example transformation: Extract relevant information from the log
    payload = log[PROTO_PAYLOAD]
    message = (
        f"Error occurred with resource {payload['resourceName']} "
        f"[{payload['methodName']}]"
    )
    timestamp = log["timestamp"]
    return ExampleOutputRow(message, timestamp)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
