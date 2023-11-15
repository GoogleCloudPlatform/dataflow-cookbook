import logging
import apache_beam as beam

from apache_beam import Map
from apache_beam.io.gcp.bigtableio import ReadFromBigtable
from apache_beam.options.pipeline_options import PipelineOptions


def run():
    """
    This Apache Beam pipeline retrieves data from Google Cloud Bigtable table.
    It performs the following steps:
    1. Read from Bigtable:
        - The pipeline reads data from a specified Bigtable instance and table.
    2. Data Transformation:
        - The rows retrieved from Bigtable are transformed to extract cell data.
    3. Logging Data:
        - The extracted cell data is logged.
    """
    class BigtableOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                '--project_id',
                default="apache-beam-testing",
                help='Input project ID'
            )
            parser.add_argument(
                '--instance_id',
                default="beam-test",
                help='Input Bigtable instance ID'
            )
            parser.add_argument(
                '--table_id',
                default="your-test-table",
                help='Input Bigtable table ID'
            )

    options = BigtableOptions()

    with beam.Pipeline(options=options) as p:

        output = (p | "Read from Bigtable" >> ReadFromBigtable(
                    project_id=options.project_id,
                    instance_id=options.instance_id,
                    table_id=options.table_id
                 )
                    | "Extract cells" >> beam.Map(lambda row: row._cells)
                    | "Log Data" >> Map(logging.info))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
