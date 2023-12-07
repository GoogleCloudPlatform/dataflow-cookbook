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
from apache_beam import Create, Map
from apache_beam.transforms.combiners import Count, Mean, Top

"""

Full list of combiners
https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.combiners.html
"""


def run(argv=None):
    elements = [
        ("Mammal", "Dog"),
        ("Mammal", "Cat"),
        ("Fish", "Salmon"),
        ("Amphibian", "Snake"),
        ("Bird", "Eagle"),
        ("Bird", "Owl"),
        ("Mammal", "Algo"),
    ]

    with beam.Pipeline() as p:
        # Counters Globally and PerKey
        input_pcol = p | Create(elements)
        element_count_total = (
            input_pcol
            | "Global Count" >> Count.Globally()
            | "Global Log" >> Map(logging.info)
        )
        element_count_grouped = (
            input_pcol
            | "Count PerKey" >> Count.PerKey()
            | "PerKey Log" >> Map(logging.info)
        )
        # Mean and Top
        numbers = p | "Create Numbers" >> Create(range(100))

        mean = numbers | Mean.Globally() | "Mean Log" >> Map(logging.info)

        top = numbers | Top.Of(5) | "Top Log" >> Map(logging.info)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
