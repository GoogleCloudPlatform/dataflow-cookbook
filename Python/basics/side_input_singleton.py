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
from apache_beam import Create, Map, pvalue


def run(argv=None):
    elements = ["Dog", "Cat", "Salmon", "Snake", "Eagle", "Owl"]
    side_element = ["s"]

    with beam.Pipeline() as p:
        side_input = p | "side input" >> Create(side_element)
        output = (
            p
            | Create(elements)
            | "Make plural"
            >> Map(
                lambda text, suffix: text + suffix,
                suffix=pvalue.AsSingleton(side_input),
            )
            | "Log" >> Map(logging.info)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
