#  Copyright 2022 Google LLC
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

from apache_beam import DoFn
from apache_beam import Map
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import Count

_QUERY = ('SELECT * '
         'FROM `bigquery-public-data.census_bureau_usa.population_by_zip_2010`')

"""

This example shows how to use Classic Templates. Note that Flex Templates are the preferred method.
"""

class CustomTemplateOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_value_provider_argument(
        '--query',
        default=_QUERY,
        help='Query to read')
    parser.add_value_provider_argument(
        '--min_population',
        default=0,
        help='Min Population')


class FilterByPopulation(DoFn):

  def __init__(self, min_population):
    self.min_population = min_population

  def process(self, element, *args, **kwargs):
    # get() method needs to be called from Runtime context i.e. not at graph
    # construction time, until the pipeline is run `self.max_number` is an
    # instance of ValueProvider, at template runtime it gets resolved to
    # parameter passed in max_number
    if element['population'] >= int(self.min_population.get()):
      yield element


def run():
  options = CustomTemplateOptions()
  p = Pipeline(options=options)

  (p | "Read from BQ" >> bigquery.ReadFromBigQuery(
                                      query=options.query,
                                      use_standard_sql=True,
                                      validate=False)
     # `get()` cannot be called here since this is not Runtime Context
     # and is a placeholder only which will resolve when template is run
     | 'Filter the elements' >> ParDo(FilterByPopulation(options.min_population))
     | 'Count globally' >> Count.Globally()
     | 'Log total' >> Map(logging.info)
   )
  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
