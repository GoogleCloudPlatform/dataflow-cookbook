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
from apache_beam import DoFn, GroupByKey, Map, ParDo, WindowInto
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream, TimestampedValue
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.window import FixedWindows


def get_input_stream():
    stream = (
        TestStream()
        .add_elements(
            [
                # timestamp value in seconds since epoch
                TimestampedValue("A", timestamp=0),
                TimestampedValue("B", timestamp=4),
                TimestampedValue("C", timestamp=6),
                TimestampedValue("D", timestamp=6),
            ]
        )
        .advance_watermark_to(9)
        .advance_processing_time(9)
        .add_elements(
            [TimestampedValue("Late-but-before-window-closing", timestamp=0)]
        )
        .advance_watermark_to(11)
        .advance_processing_time(3)
        .add_elements(
            [
                # Although pane fired, below element still within lateness configured
                TimestampedValue("Late-but-allowed", timestamp=9)
            ]
        )
        .advance_watermark_to(15)
        .advance_processing_time(4)
        .add_elements(
            [
                TimestampedValue("Late-and-outside-window", timestamp=9),
                TimestampedValue("Window2-E", timestamp=11),
            ]
        )
        .advance_watermark_to_infinity()
    )
    return stream


class FormatElements(DoFn):
    # Formats the elements and adds metadata information to the pane
    def process(
        self, element, window=DoFn.WindowParam, pane=DoFn.PaneInfoParam
    ):
        yield "Elements in pane: {} \nWindow {} - Pane Index {}\n".format(
            element[1], window, pane.index
        )


def run():
    options = PipelineOptions(streaming=True)

    p = TestPipeline(options=options)

    window_size_seconds = 10
    window_allowed_lateness_seconds = 3

    stream = get_input_stream()

    (
        p
        | stream
        | "Assign elements within pane same key" >> Map(lambda e: ("Key", e))
        | "Window into Discarding FixedWindows"
        >> WindowInto(
            FixedWindows(size=window_size_seconds),
            allowed_lateness=window_allowed_lateness_seconds,
            # Set `accumulation_mode` as `ACCUMULATING` when you want to collect all the
            # panes fired in the window. To have the panes be considered individually
            # use `DISCARDING` mode. Use `DISCARDING` when the datum are indipendent
            # of each other. `ACCUMULATING` will fire each time the a panes arrives from a window
            accumulation_mode=AccumulationMode.DISCARDING,
        )
        | "Group together elements within same pane and key" >> GroupByKey()
        | "Format elements" >> ParDo(FormatElements())
        | Map(print)
    )

    p.run()


if __name__ == "__main__":
    logging.getLogger("LateData").setLevel(logging.INFO)
    run()


#   EXPLANATION
# 1 - Elements A,B,C,D arrive to the pipeline on time
# 1 - Element "Late-but-before-window-closing" arrives 9s late, but before window closes
# 2 - Element "Late-but-allowed" arrives after window closing, but within the allowed late time
# 3 - Element "Late-and-outside-window" arrives after window closing and after allowed late time
# 4 - Element "window-2" arrives on time but in a different window than 0 to 10000
#
# The first trigger with elements (1) contains all data from the first window that arrived before window closed
# The second trigger with element (2) includes the data that arrived late but within the allowed late time
# Elements in (3) are discarded, since they arrive after the allowed late time
# The third trigger with element (4) appears in a new window `[10.0, 20.0)` with pane index `0`
# Even though the Window is same for (1) (2) the pane index for (1) is `0` and for (2) is `1`
# denoting they are logically separated
