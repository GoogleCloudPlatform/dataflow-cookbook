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
from apache_beam.transforms.window import (
    FixedWindows,
    TimestampCombiner,
    TimestampedValue,
)


def get_input_stream():
    stream = (
        TestStream()
        .add_elements([TimestampedValue("First", timestamp=0)])
        .advance_watermark_to(2)
        .advance_processing_time(2)
        .add_elements([TimestampedValue("Second", timestamp=1)])
        .advance_watermark_to(3)
        .advance_processing_time(1)
        .add_elements(
            [
                TimestampedValue("Third", timestamp=2),
                TimestampedValue("Fourth", timestamp=4),
                TimestampedValue("Fifth", timestamp=4.5),
            ]
        )
        # change 6 -> 4 and processing time 3 -> 1, to see AfterProcessingTime functionality
        .advance_watermark_to(6)
        .advance_processing_time(3)
        .add_elements(
            [
                TimestampedValue("Sixth", timestamp=8),
                TimestampedValue("Last-In-Window", timestamp=9),
            ]
        )
        .advance_watermark_to(11)
        .advance_processing_time(5)
        .add_elements(
            [
                TimestampedValue("Window2-First", timestamp=11),
                TimestampedValue("Late-1", timestamp=1),
                TimestampedValue("Late-2", timestamp=3),
            ]
        )
        .advance_watermark_to(12)
        .advance_processing_time(1)
        .add_elements(
            [
                TimestampedValue("Window2-Second", timestamp=12),
                TimestampedValue("Late-3", timestamp=5),
            ]
        )
        .advance_watermark_to(13)
        .advance_processing_time(1)
        .add_elements(
            [
                TimestampedValue("Late-4", timestamp=6),
            ]
        )
        .advance_watermark_to(13.5)
        .advance_processing_time(0.5)
        .add_elements(
            [
                TimestampedValue("Late-5", timestamp=6),
            ]
        )
        .advance_watermark_to(14)
        .advance_processing_time(0.5)
        .add_elements([TimestampedValue("Late-6", timestamp=9)])
        .advance_watermark_to(14.5)
        .advance_processing_time(0.5)
        .add_elements([TimestampedValue("Late-7", timestamp=9)])
        .advance_watermark_to(16)
        .advance_processing_time(2)
        .add_elements(
            [
                TimestampedValue("Outside-Allowed-Lateness", timestamp=4),
                TimestampedValue("Window2-Third", timestamp=15),
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
    delay = 3
    late_pane_size = 3

    stream = get_input_stream()

    (
        p
        | stream
        | "Assign elements within pane same key" >> Map(lambda e: ("Key", e))
        | "Window into FixedWindow with early+late fires"
        >> WindowInto(
            FixedWindows(window_size_seconds),
            allowed_lateness=window_allowed_lateness_seconds,
            accumulation_mode=trigger.AccumulationMode.DISCARDING,
            trigger=trigger.AfterWatermark(
                # see explanation below for `early` and `late`
                early=trigger.AfterProcessingTime(delay=delay),
                late=trigger.AfterEach(
                    trigger.AfterCount(count=late_pane_size),
                    trigger.AfterCount(1),
                ),
            ),
            timestamp_combiner=TimestampCombiner.OUTPUT_AT_EARLIEST,
        )
        | "Combine elements within pane by key" >> GroupByKey()
        | "Print elements" >> Map(print)
    )
    p.run()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()


# EXPLANATION
#
# By default, `trigger.AfterWatermark()` triggers when the the window closes and
# fires whenever late element are encountered, but the default behaviour can be
# overridden using `early` and `late`, where `early` deals with triggering logic for elements within
# window and `late` deals with triggering logic for late data
#
# 1 - The first trigger contains "First" to "Fifth", which are the early firings because of
#   `trigger.AfterProcessingTime(delay=delay)`, so all elements that arrived from 0 to 5.
# 2 - Second trigger contains the rest of the elements that arrived on time before the window closed
# 3 - Since `late` uses `AfterEach` with two triggers of `AfterCount` but with different
#   count size. Third pane triggers when the pane has at-least 3 elements
# 4 - Fourth trigger only contains "Late-4" since second trigger is `trigger.AfterCount(1)` from the `AfterEach`
# 5 - "Late-5", "Late-6", "Late-7" is triggered because `trigger.AfterCount(count=late_pane_size)` is triggered again
#         -> See "after_each_trigger.py" to see working for `AfterEach`
