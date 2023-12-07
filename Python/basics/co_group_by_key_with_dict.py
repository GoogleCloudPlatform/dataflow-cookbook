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
from apache_beam import CoGroupByKey, Create, Map


def run(argv=None):
    jobs = [
        ("Anna", "SWE"),
        ("Kim", "Data Engineer"),
        ("Kim", "Data Scientist"),
        ("Robert", "Artist"),
        ("Sophia", "CEO"),
    ]
    hobbies = [
        ("Anna", "Painting"),
        ("Kim", "Football"),
        ("Kim", "Gardening"),
        ("Robert", "Swimming"),
        ("Sophia", "Mathematics"),
        ("Sophia", "Tennis"),
    ]

    with beam.Pipeline() as p:
        jobs_pcol = p | "Create Jobs" >> Create(jobs)
        hobbies_pcol = p | "Create Hobbies" >> Create(hobbies)
        output = (
            {"Jobs": jobs_pcol, "Hobbies": hobbies_pcol}
            | CoGroupByKey()
            | "Log" >> Map(logging.info)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
