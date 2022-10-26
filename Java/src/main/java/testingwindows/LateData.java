/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package testingwindows;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LateData {
    private static final Logger LOG = LoggerFactory.getLogger(LateData.class);

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        Integer windowLength = 10;
        Integer allowLateSize = 3;

        TestStream<String> streamEvents = TestStream.create(StringUtf8Coder.of())
                .addElements(
                        TimestampedValue.of("A", new Instant(0)),
                        TimestampedValue.of("B", new Instant(4000)), // Value of Instant is in millis
                        TimestampedValue.of("C", new Instant(6000)),
                        TimestampedValue.of("D", new Instant(6000))
                )
                // Move the watermark past the end the end of the window
                .advanceWatermarkTo(new Instant(9000))
                .advanceProcessingTime(Duration.standardSeconds(9))
                .addElements(
                        // element arrives at second 9 but with timestamp 0
                        TimestampedValue.of("Late-but-before-window-closing", new Instant(0))
                )
                .advanceWatermarkTo(new Instant(11000))// move Watermark time to sec 11
                .advanceProcessingTime(Duration.standardSeconds(2)) // move Processing time to sec 11
                .addElements(
                        // element arrives at sec 11 but with ts 8, outside the window but within allowed lateness
                        TimestampedValue.of("Late-but-allowed", new Instant(9000))
                )
                .advanceWatermarkTo(new Instant(15000))
                .advanceProcessingTime(Duration.standardSeconds(4)) // move Processing time to sec 15
                .addElements(
                        // element arrives at sec 15 but with ts 9, outside the window
                        TimestampedValue.of("Late-outside-window-and-latedata", new Instant(9000)),
                        TimestampedValue.of("window2", new Instant(14000))

                )
                .advanceWatermarkToInfinity();

        p.apply(streamEvents)
                .apply("KVs", ParDo.of(new DoFn<String, KV<String, KV<String, Instant>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, Instant> element = KV.of(c.element(), c.timestamp());
                        c.output(KV.of("key", element));
                    }
                }))
                .apply(Window.<KV<String, KV<String, Instant>>>into(
                        FixedWindows.of(Duration.standardSeconds(windowLength)))
                        .withAllowedLateness(Duration.standardSeconds(allowLateSize))
                        .discardingFiredPanes()
                )
                .apply(GroupByKey.create())
                .apply("Log", ParDo.of(new DoFn<KV<String, Iterable<KV<String, Instant>>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("TRIGGER \n" + c.element().getValue().toString()
                                + ". \n Window end " + c.timestamp().plus(1));
                        c.output(c.pane().toString());
                    }
                }));

        p.run();
    }
}

        /*
        EXPLANATION
        1 - Elements A,B,C,D arrive to the pipeline on time
        1 - Element "Late-but-before-window-closing" arrives 9s late, but before window closes
        2 - Element "Late-but-allowed" arrives after window closing, but within the allowed late time
        3 - Element "Late-outside-window-and-latedata" arrives after window closing and after allowed late time
        4 - Element "window-2" arrives on time but in a different window than 0 to 10000

        The first trigger with elements (1) contains all data from the first window that arrived before window closed
        The second trigger with element (2) includes the data that arrived late but within the allowed late time
        Elements in (3) are discarded, since they arrive after the allowed late time
        The third trigger with element (4) appears in a new windows that closes at 1970-01-01T00:00:20.000Z
         */
