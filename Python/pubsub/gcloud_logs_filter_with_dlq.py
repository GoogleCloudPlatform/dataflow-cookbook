import logging
import json

import apache_beam as beam
from apache_beam import DoFn
from apache_beam import Filter
from apache_beam import Map
from apache_beam import ParDo
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    """
    This Apache Beam pipeline processes log messages from a Google Cloud Pub/Sub topic.
    The expected data format follows the standard Google Cloud log format,
    which can be achieved by routing logs to a Pub/Sub topic via https://console.cloud.google.com/logs/router.

    It performs the following steps:
    1. Input Configuration:
        - Reads messages from the specified input Pub/Sub topic.

    2. Message Parsing:
        - Parses each message as a JSON dictionary.

    3. Message Splitting:
        - Divides messages into two categories:
        a. 'processed' (contain both 'severity' and 'jsonPayload' fields)
        b. 'unprocessed' (missing one or both of these fields).

    4. Severity Filtering:
        - For 'processed' messages, filters out those with severity other than "ERROR".

    5. Data Transformation:
        - Extracts timestamp and message content from the 'jsonPayload' field for 'processed' messages.

    6. Output Handling:
        - Writes transformed 'processed' messages to a specified output Pub/Sub topic.
        - Sends 'unprocessed' messages to a Dead Letter Queue (DLQ) topic.
    """
    class PubSubOptions(PipelineOptions):

        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                "--input_topic",
                default="projects/apache-beam-testing/topics/vterentev-input-test",
                help="Input PubSub topic")
            parser.add_argument(
                "--output_topic",
                default="projects/apache-beam-testing/topics/vterentev-output-test",
                help="Output PubSub topic")
            parser.add_argument(
                "--dlq_topic",
                default="projects/apache-beam-testing/topics/vterentev-dlq-test",
                help="Dead Letter Queue PubSub topic")

    options = PubSubOptions(streaming=True)

    with beam.Pipeline(options=options) as p:
        split_result = (p | "Read from PubSub" >> ReadFromPubSub(topic=options.input_topic)
                            | "Parse JSON" >> Map(lambda msg: json.loads(msg))
                            | "Split Messages" >> ParDo(SplitMessages()).with_outputs("unprocessed", "processed"))

        # Filter processed messages and write to output topic
        (split_result['processed']
         | "Filter by Severity" >> Filter(filter_by_severity)
         | "Transform Data" >> Map(transform_data)
         | "Write to PubSub" >> WriteToPubSub(options.output_topic, with_attributes=True))

        # Write unprocessed messages to DLQ
        (split_result['unprocessed']
         | "Map to PubsubMessage" >> Map(to_pubsub_message)
         | "Write to DLQ" >> WriteToPubSub(options.dlq_topic, with_attributes=True))

class SplitMessages(DoFn):
    def process(self, element):
        from apache_beam.pvalue import TaggedOutput

        if ('severity' in element) & ('jsonPayload' in element):
            yield TaggedOutput('processed', element)
        else:
            yield TaggedOutput('unprocessed', element)

def filter_by_severity(log):
    # Filter logs by severity level (e.g., only process logs with severity "ERROR")
    return log.get("severity").upper() == "ERROR"

def to_pubsub_message(msg):
    from apache_beam.io import PubsubMessage

    return PubsubMessage(data=bytes(json.dumps(msg), "utf-8"), attributes=None)

def transform_data(log):
    from apache_beam.io import PubsubMessage

    # Example transformation: Extract relevant information from the log
    transformed_data = {
        "timestamp": log.get("timestamp"),
        "message": log.get("jsonPayload").get("message")
    }
    data = bytes(f"Error log message: {transformed_data['message']} [{transformed_data['timestamp']}]", "utf-8")
    return PubsubMessage(data=data, attributes=transformed_data)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()