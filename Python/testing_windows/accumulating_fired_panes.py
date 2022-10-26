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
from apache_beam import GroupByKey
from apache_beam import Map
from apache_beam import WindowInto
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.windowed_value import PaneInfoTiming


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

def print_with_info(element, window=DoFn.WindowParam, pane=DoFn.PaneInfoParam):
  """Helper function to print window and pane info"""
  print(f"Element: {element[1]} \n\t Window: {window} \n\t Pane Timing: {PaneInfoTiming.to_string(pane.timing)}")


def run():
  options = PipelineOptions(streaming=True)

  p = TestPipeline(options=options)

  window_size_seconds = 10
  window_allowed_lateness_seconds = 5

  stream = get_input_stream()

  (p | stream
     | "Assign elements within pane same key" >> Map(lambda e: ("key", e))
     | "Window into FixedWindows" >> WindowInto(
          # We are using the default triggering which triggers when watermark
          # crosses the window end and whenever valid late element(s) are added
          FixedWindows(size=window_size_seconds),
          allowed_lateness=window_allowed_lateness_seconds,
          # Late elements are combined with previous elements within the window
          accumulation_mode=AccumulationMode.ACCUMULATING)
     | "Combine elements within pane by key" >> GroupByKey()
     | Map(print_with_info)
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
The second trigger contains elements (2) and elements (1), since we are accumulating
The third trigger contains only elements (3) but also (2) and (1) because of the accumulation
"""
