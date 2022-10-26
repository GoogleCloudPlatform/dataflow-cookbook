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

from apache_beam import GroupByKey
from apache_beam import Map
from apache_beam import WindowInto
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import TimestampedValue


def get_input_stream():
  stream = (
      # timestamp = seconds since epoch
      TestStream().add_elements([
          TimestampedValue("On-Time-1", timestamp=0),
          TimestampedValue("On-Time-2", timestamp=4),
          TimestampedValue("On-Time-3", timestamp=6)
      ])
      .advance_watermark_to(6)  # move watermark to epoch sec 6
      .advance_processing_time(6)  # move processed time to epoch sec 6
      .add_elements([
          # Elements arrive out of order but still within window
          TimestampedValue("On-Time-4", timestamp=4),
          TimestampedValue("On-Time-5", timestamp=7)
      ])
      .advance_watermark_to(11)
      .advance_processing_time(5)
      # processing time crosses window, pane triggered containing 5 elements
      .add_elements([
          TimestampedValue("Late-1", timestamp=0),
          TimestampedValue("Late-2", timestamp=5),
          TimestampedValue("Late-3", timestamp=6),
          TimestampedValue("Late-4", timestamp=7)
      ])
      .advance_watermark_to(15)
      .advance_processing_time(4)
      # processing time within allowed lateness window, pane triggered again
      .add_elements([
          TimestampedValue("Late-5", timestamp=9),
          TimestampedValue("Late-6", timestamp=9),
      ])
      .advance_watermark_to(16)
      .advance_processing_time(1)
      # processing time moves from 14 -> 16, 14 still within lateness window
      .add_elements([
          TimestampedValue("Outside-allowed-lateness", timestamp=9),
          TimestampedValue("On-Time-New-Window-1", timestamp=17)
      ])
      # processing time crossed allowed lateness, first element not triggered
      # On-Time-New-Window-1 shows this process will continue until stopped
      .advance_watermark_to_infinity()
  )

  return stream


def run():
  options = PipelineOptions(streaming=True)

  p = TestPipeline(options=options)

  window_size_seconds = 10
  window_allowed_lateness_seconds = 5

  stream = get_input_stream()

  (p | stream
     | "Assign elements within pane same key" >> Map(lambda e: ("key", e))
     | "Window into FixedWindows" >> WindowInto(
          FixedWindows(size=window_size_seconds),
          allowed_lateness=window_allowed_lateness_seconds,
          # Late elements are contained within their own pane and no correlation
          # exists between the panes
          accumulation_mode=AccumulationMode.DISCARDING)
     | "Combine elements within pane by key" >> GroupByKey()
     | "Print elements" >> Map(lambda ele: print('TRIGGER ', ele[1]))
   )

  p.run()


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()

"""

EXPLANATION
  1 - Elements "On-Time-X" arrive to the pipeline before window closes
  2 - Elements "Late-(1 to 4)" arrive after window closing, but within the allowed late time
  3 - Elements "Late-(5, 6)" also arrive after window closing, but within the allowed late time
  4 - Element "Outside-allowed-lateness" is discarded as it's outside the allowed lateness

  The first trigger contains elements (1), since they arrive before window closing
  The second trigger contains elements (2) and only elements (2). They are triggered by themselves as they arrived late
  The thrid trigger contains only elements (3) since they also arrived late but at different time

Element "On-Time-New-Window-1" shows that this process continues until termination
"""

