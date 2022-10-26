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

public class AccumulatingFiredPanes {
    private static final Logger LOG = LoggerFactory.getLogger(AccumulatingFiredPanes.class);

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        Integer windowLength = 10;
        Integer allowLateSize = 5;

        // Behaviour is explained in detail at the end of the file
        // Note: all elements would fall into the same window
        TestStream<String> streamEvents = TestStream.create(StringUtf8Coder.of())
                .addElements(
                        TimestampedValue.of("On-time-1", new Instant(0)),
                        TimestampedValue.of("On-time-2", new Instant(4000)), // Value of Instant is in millis
                        TimestampedValue.of("On-time-3", new Instant(6000))

                )
                .advanceWatermarkTo(new Instant(6000))// move Watermark time to sec 6
                .advanceProcessingTime(Duration.standardSeconds(6)) // move Processing time to sec 6
                .addElements(
                        TimestampedValue.of("On-time-4", new Instant(4000)),
                        TimestampedValue.of("On-time-5", new Instant(7000))
                )
                .advanceWatermarkTo(new Instant(11000))// move Watermark time to sec 11
                .advanceProcessingTime(Duration.standardSeconds(5)) // move Processing time to sec 11
                .addElements(
                        // element arrives at sec 11 but fall in the window
                        TimestampedValue.of("Late-1", new Instant(0)),
                        TimestampedValue.of("Late-2", new Instant(5000)),
                        TimestampedValue.of("Late-3", new Instant(6000)),
                        TimestampedValue.of("Late-4", new Instant(7000))
                )
                .advanceWatermarkTo(new Instant(14000))
                .advanceProcessingTime(Duration.standardSeconds(3)) // move Processing time to sec 14
                .addElements(
                        TimestampedValue.of("Late-5", new Instant(9000)),
                        TimestampedValue.of("Late-6", new Instant(9000))

                )
                .advanceWatermarkTo(new Instant(16000))
                .advanceProcessingTime(Duration.standardSeconds(2)) // move Processing time to sec 14
                .addElements(
                        TimestampedValue.of("Outside-allowed-lateness", new Instant(9000))

                )
                .advanceWatermarkToInfinity();

        p.apply(streamEvents)
                .apply("KVs", ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(KV.of("key", c.element()));
                    }
                }))
                .apply(Window.<KV<String, String>>into(
                        FixedWindows.of(Duration.standardSeconds(windowLength)))
                        .withAllowedLateness(Duration.standardSeconds(allowLateSize))
                        .accumulatingFiredPanes() // New fired elements would be merged with previous fired elements

                )
                .apply(GroupByKey.create())
                .apply("Log", ParDo.of(new DoFn<KV<String, Iterable<String>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("\n TRIGGER " + c.element().getValue().toString());
                        c.output(c.pane().toString());
                    }
                }));


        p.run();
    }
}
        /*
        EXPLANATION
        1 - Elements On-Time-X arrive to the pipeline before window closes
        2 - Elements "Late-(1 to 4)" arrive after window closing, but within the allowed late time
        3 - Elements "Late-(5, 6)" also arrive after window closing, but within the allowed late time
        4 - Element "Outside-allowed-lateness" is discarded as it's outside the allowed lateness


        The first trigger contains elements (1), since they arrive before window closing
        The second trigger contains elements (2) and elements (1), since we are accumulating
        The third trigger contains only elements (3) but also (2) and (1) because of the accumulation
         */
