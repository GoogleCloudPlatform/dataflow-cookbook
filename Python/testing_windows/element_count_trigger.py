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
import apache_beam.transforms.trigger as trigger
from apache_beam import GroupByKey, Map, WindowInto
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import FixedWindows, TimestampedValue


def get_input_stream():
    stream = (
        TestStream()
        .add_elements(
            [
                # timestamp in seconds since epoch
                TimestampedValue("First", timestamp=0)
            ]
        )
        .advance_watermark_to(2)
        .advance_processing_time(2)
        .add_elements(
            [
                # late from pipeline perspective but still within window
                TimestampedValue("Second", timestamp=1)
            ]
        )
        .advance_watermark_to(4)
        .advance_processing_time(2)
        .add_elements([TimestampedValue("Third", timestamp=3)])
        .advance_watermark_to(6)
        .advance_processing_time(2)
        .add_elements(
            [
                # Different event times but elements "arrived" same time in pipeline
                TimestampedValue("Same-Time-1", timestamp=4),
                TimestampedValue("Same-Time-2", timestamp=5),
                TimestampedValue("Same-Time-3", timestamp=6),
            ]
        )
        .advance_watermark_to(9)
        .advance_processing_time(3)
        .add_elements([TimestampedValue("Last-In-Window", timestamp=8)])
        .advance_watermark_to(11)
        .advance_processing_time(3)
        .add_elements(
            [
                TimestampedValue("Late-1", timestamp=0),
                TimestampedValue("Late-2", timestamp=5),
                TimestampedValue("Late-3", timestamp=6),
            ]
        )
        .advance_watermark_to(12)
        .advance_processing_time(1)
        .add_elements([TimestampedValue("Late-4", timestamp=7)])
        .advance_watermark_to(13)
        .advance_processing_time(1)
        .add_elements([TimestampedValue("Late-5", timestamp=6)])
        .advance_watermark_to(14)
        .advance_processing_time(1)
        .add_elements([TimestampedValue("Late-6", timestamp=0)])
        .advance_watermark_to(15)
        .advance_processing_time(1)
        .add_elements([TimestampedValue("Late-7", timestamp=1)])
        .advance_watermark_to(16)
        .advance_processing_time(1)
        .add_elements(
            [
                TimestampedValue("Outside-Window-Lateness", timestamp=4),
                TimestampedValue("Window-2-In-Time-1", timestamp=14),
            ]
        )
        .advance_watermark_to_infinity()
    )
    return stream


def run():
    options = PipelineOptions(streaming=True)

    p = TestPipeline(options=options)

    window_size_seconds = 10
    window_allowed_lateness_seconds = 5
    late_pane_size = 2

    stream = get_input_stream()

    (
        p
        | stream
        | "Assign elements within pane same key" >> Map(lambda e: ("Key", e))
        | "Window into FixedWindow with triggering"
        >> WindowInto(
            FixedWindows(window_size_seconds),
            allowed_lateness=window_allowed_lateness_seconds,
            accumulation_mode=trigger.AccumulationMode.DISCARDING,
            # Try setting different types of triggers!
            # AfterWatermark only fires pane once when the watermark crosses the
            # Window end time
            # trigger=trigger.AfterWatermark()
            # We  repeatedly fire panes forever with count as least late_pane_size
            # We also want to set a termination to stop above infinite fires so
            # use AfterWatermark a final trigger.
            # trigger=trigger.OrFinally(
            #     trigger.Repeatedly(trigger.AfterCount(late_pane_size)),
            #     trigger.AfterWatermark()
            # )
            # We need to configure how late data should be handled to prevent data
            #  loss since AfterWatermark by default only fires once
            # https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/trigger.py#L596
            trigger=trigger.OrFinally(
                trigger.Repeatedly(trigger.AfterCount(late_pane_size)),
                #   trigger.Repeatedly automatically called for `late` param
                trigger.AfterWatermark(late=trigger.AfterCount(1)),
            ),
        )
        | "Combine elements within pane by key" >> GroupByKey()
        | "Print elements" >> Map(lambda e: print("TRIGGER ", e[1]))
    )

    p.run()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

"""
EXPLANATION

`trigger.AfterCount` triggers whenever the current pane has at least `late_pane_size` elements,
 but there are some constrains.

1 - The first trigger contains "First" and "Second", which is expected since they are the first 2 elements.
2 - The second trigger contains the "Third" element but also all of "Same-Time-X" elements, so it is not just 2 elements.
  This happens because of "Same-Time-X" arrive at the same time, so there is no second element but second elements
3 - The third trigger only contains "Last-In-Window", this is because we have `trigger.OrFinally(... , trigger.AfterWatermark(...))`
  so it triggers when the window closes, no matter the number of elements there are in pane.
4 - The fourth trigger contains "Late-(1,2,3)", since they arrive together
5 - The next triggers contain the elements as they come, since we have `trigger.OrFinally(... , trigger.AfterWatermark(...))`
6 - Element "Outside-Window-Lateness" is discarded as it's outside the allowed lateness

In a real life scenario, it's not that common two elements arrive at the exact same time, but networking conditions can affect arrival.
Note: `AfterCount` can cause hanged pipeline if the condition is not met. To resolve this see note in `after_each_trigger.py`
"""
