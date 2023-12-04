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

import apache_beam as beam
from apache_beam import CoGroupByKey
from apache_beam import Create
from apache_beam import Map


def run(argv=None):
  # Define the input key-value pairs.
  jobs = [("Anna", "SWE"), ("Kim", "Data Engineer"), ("Kim", "Data Scientist"),
          ("Robert", "Artist"), ("Sophia", "CEO")]
  hobbies = [("Anna", "Painting"), ("Kim", "Football"), ("Kim", "Gardening"),
             ("Robert", "Swimming"), ("Sophia", "Mathematics"),
             ("Sophia", "Tennis")]

  # Create two PCollections with beam.Create().
  # Use beam.CoGroupByKey() to group elements from these two PCollections
  # by their key.
  with beam.Pipeline() as p:
    jobs_pcol = p | "Create Jobs" >> Create(jobs)
    hobbies_pcol = p | "Create Hobbies" >> Create(hobbies)
    _ = ((jobs_pcol, hobbies_pcol) | "CoGroupByKey" >> CoGroupByKey()
         | "Log" >> Map(logging.info))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
