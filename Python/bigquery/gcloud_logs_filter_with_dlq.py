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
import json

import apache_beam as beam
from apache_beam import DoFn
from apache_beam import Filter
from apache_beam import Map
from apache_beam import ParDo
from apache_beam.io import ReadFromBigQuery
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions


PROCESSED_TAG = "processed"
UNPROCESSED_TAG = "unprocessed"


class BigQueryOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_table",
            default="your-input-table",
            help="Input Google Cloud BigQuery table")
        parser.add_argument(
            "--output_table",
            default="your-output-table",
            help="Output Google Cloud BigQuery table")
        parser.add_argument(
            "--dlq_table",
            default="your-dlq-table",
            help="Dead Letter Queue Google Cloud BigQuery table")


def run():
    """
    This Apache Beam pipeline processes log messages from a Google Cloud BigQuery table.
    The expected data format follows the standard Google Cloud log format,
    which can be achieved by routing logs to a BigQuery dataset via https://console.cloud.google.com/logs/router.

    It performs the following steps:
    1. Input Configuration:
        - Reads messages from the specified input BigQuery table.

    2. Message Splitting:
        - Divides messages into two categories:
        a. PROCESSED (contain both 'severity' and 'protopayload_auditlog' fields)
        b. UNPROCESSED (missing one or both of these fields).

    4. Severity Filtering:
        - For PROCESSED messages, filters out those with severity other than "ERROR".

    5. Data Transformation:
        - Extracts timestamp and substantial content from the 'protopayload_auditlog' field for PROCESSED messages.

    6. Output Handling:
        - Writes transformed PROCESSED messages to a specified output BigQuery table.
        - Sends UNPROCESSED messages to a Dead Letter Queue (DLQ) BigQuery table.
    """
    options = BigQueryOptions(streaming=True)
    output_table_schema = "message:STRING, timestamp:TIMESTAMP"
    dlq_table_schema = "resource:STRING, timestamp:TIMESTAMP"

    with beam.Pipeline(options=options) as p:
        split_result = (p | "Read from BigQuery" >> ReadFromBigQuery(table=options.input_table)
                            | "Split Messages" >> ParDo(SplitMessages()).with_outputs(UNPROCESSED_TAG, PROCESSED_TAG))

        # Filter processed messages and write to output topic
        (split_result[PROCESSED_TAG]
         | "Filter by Severity" >> Filter(filter_by_severity)
         | "Map to Row for output" >> Map(to_row_for_output)
         | "Write to BigQuery" >> WriteToBigQuery(options.output_table,
                                                 schema=output_table_schema))

        # Write unprocessed messages to DLQ
        (split_result[UNPROCESSED_TAG]
         | "Map to Row for DLQ" >> Map(to_row_for_dlq)
         | "Write to DLQ" >> WriteToBigQuery(options.dlq_table,
                                           schema=dlq_table_schema))


class SplitMessages(DoFn):
    def process(self, element):
        from apache_beam.pvalue import TaggedOutput

        if ('severity' in element) & ('protopayload_auditlog' in element):
            yield TaggedOutput(PROCESSED_TAG, element)
        else:
            yield TaggedOutput(UNPROCESSED_TAG, element)


def filter_by_severity(log):
    # Filter logs by severity level (e.g., only process logs with severity "ERROR")
    return log.get("severity").upper() == "ERROR"


def to_row_for_dlq(log):
    data = {
        "resource": json.dumps(log.get('resource')),
        "timestamp": log.get('timestamp')
    }
    return data


def to_row_for_output(log):

    # Example transformation: Extract relevant information from the log
    payload = log.get("protopayload_auditlog")
    message = f"Error occurred with resource {payload.get('resourceName')} [{payload.get('methodName')}]"
    transformed_data = {
        "timestamp": log.get("timestamp"),
        "message": message
    }
    return transformed_data


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
