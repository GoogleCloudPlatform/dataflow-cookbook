import logging
import apache_beam as beam

from apache_beam import Create
from apache_beam import Map
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from apache_beam.options.pipeline_options import PipelineOptions


def run():
    """
    This Apache Beam pipeline transforms and writes sample data into Google Cloud Bigtable table.
    It performs the following steps:
    1. Input Data Generation:
        - Simulated data is created as a sample input dataset.
    2. Data Transformation to Bigtable Rows:
        - Each input element is converted into a Bigtable DirectRow format.
    3. Writing to Bigtable:
        - The transformed Bigtable rows are written to a specified Bigtable instance and table.
    """
    class BigtableOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                '--project_id',
                default="apache-beam-testing",
                help='Output project ID'
            )
            parser.add_argument(
                '--instance_id',
                default="beam-test",
                help='Output Bigtable instance ID'
            )
            parser.add_argument(
                '--table_id',
                default="your-test-table",
                help='Output Bigtable table ID'
            )

    options = BigtableOptions()

    with beam.Pipeline(options=options) as p:
        # Create sample data
        elements = [
            (1, "Charles, 1995, USA"),
            (2, "Alice, 1997, Spain"),
            (3, "Bob, 1995, USA"),
            (4, "Amanda, 1991, France"),
            (5, "Alex, 1999, Mexico"),
            (6, "Eliza, 2000, Japan")
        ]

        output = (p | "Create elements" >> Create(elements)
                    | "Map to BigTable Row" >> Map(make_bigtable_row)
                    | "Write to BigTable" >> WriteToBigTable(
            project_id=options.project_id,
            instance_id=options.instance_id,
            table_id=options.table_id)
         )


def make_bigtable_row(element):
    from google.cloud.bigtable.row import DirectRow
    from datetime import datetime

    # Example transformation to BigTable DirectRow

    index = element[0]
    row_fields = element[1].split(", ")
    column_family_id = 'cf1'
    key = "beam_key%s" % ('{0:07}'.format(index))
    direct_row = DirectRow(row_key=key)
    for column_id, value in enumerate(row_fields):
        direct_row.set_cell(
            column_family_id, ('field%s' % column_id).encode('utf-8'),
            value,
            datetime.now())
    return direct_row


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
