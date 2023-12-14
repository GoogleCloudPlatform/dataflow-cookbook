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

# standard libraries
import logging

# third party libraries
import apache_beam as beam
from apache_beam import Create, DoFn, Map, ParDo, pvalue
from apache_beam.options.pipeline_options import PipelineOptions


class SplitFn(DoFn):
    def process(self, element):
        # Generate 3 PCollections from the input:
        # 1) Even elements, with the 'even' tag
        # 2) Odd elements, with the 'odd' tag
        # 3) All elements emitted as the main untagged output
        if element % 2 == 0:
            yield pvalue.TaggedOutput("even", element)
        else:
            yield pvalue.TaggedOutput("odd", element)
        yield element


def run(argv=None):
    n = 20
    options = PipelineOptions(save_main_session=True)
    with beam.Pipeline(options=options) as p:
        output = (
            p
            | Create(range(n))
            | "Split Output" >> ParDo(SplitFn()).with_outputs("even", "odd")
        )

        # Log each element of both tagged PCollections
        # and the main untagged PCollection
        odd = output.odd | "odd log" >> Map(
            lambda x: logging.info("odds %d" % x)
        )
        even = output.even | "even log" >> Map(
            lambda x: logging.info("evens %d" % x)
        )
        all_output = output[None] | "Log" >> Map(
            lambda x: logging.info("all %d" % x)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
