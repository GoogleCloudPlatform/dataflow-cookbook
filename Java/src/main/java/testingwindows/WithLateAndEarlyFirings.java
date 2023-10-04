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
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WithLateAndEarlyFirings {
    private static final Logger LOG = LoggerFactory.getLogger(ElementCountTrigger.class);

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        Integer windowLength = 10;
        Integer allowLateSize = 5;
        Duration delay = Duration.standardSeconds(5);
        Integer countPass = 3; // Trigger if 3 elements pass the window

        // Behaviour is explained in detail at the end of the file
        // Note: all elements would fall into the same window
        TestStream<String> streamEvents = TestStream.create(StringUtf8Coder.of())
                .addElements(
                        TimestampedValue.of("first", new Instant(0))
                )
                .advanceWatermarkTo(new Instant(2000))
                .advanceProcessingTime(Duration.standardSeconds(2))
                .addElements(
                        TimestampedValue.of("second", new Instant(1000))
                )
                .advanceWatermarkTo(new Instant(4000))
                .advanceProcessingTime(Duration.standardSeconds(2))
                .addElements(
                        TimestampedValue.of("third", new Instant(3000))
                )
                .advanceWatermarkTo(new Instant(6000))
                .advanceProcessingTime(Duration.standardSeconds(2))
                .addElements(
                        TimestampedValue.of("fourth", new Instant(4000)),
                        TimestampedValue.of("fifth", new Instant(5000))
                )
                .advanceWatermarkTo(new Instant(9000))
                .advanceProcessingTime(Duration.standardSeconds(3))
                .addElements(
                        TimestampedValue.of("last-in-window", new Instant(8000))
                )
                .advanceWatermarkTo(new Instant(11000))
                .advanceProcessingTime(Duration.standardSeconds(2))
                .addElements(
                        TimestampedValue.of("Late-1", new Instant(0)),
                        TimestampedValue.of("Late-2", new Instant(5000))
                )
                .advanceWatermarkTo(new Instant(12000))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(
                        TimestampedValue.of("Late-3", new Instant(5000))
                )
                .advanceWatermarkTo(new Instant(13000))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(
                        TimestampedValue.of("Late-4", new Instant(5000)),
                        TimestampedValue.of("Late-5", new Instant(8000))

                )
                .advanceWatermarkTo(new Instant(14000))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(
                        TimestampedValue.of("Late-6", new Instant(9000))

                )
                .advanceWatermarkTo(new Instant(14500))
                .advanceProcessingTime(Duration.millis(500))
                .addElements(
                        TimestampedValue.of("Late-7", new Instant(9000))

                )
                .advanceWatermarkTo(new Instant(16000))
                .advanceProcessingTime(Duration.standardSeconds(2)) // move Processing time to sec 16
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
                        .triggering(
                                AfterWatermark.pastEndOfWindow()
                                        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(delay))
                                        .withLateFirings(AfterPane.elementCountAtLeast(countPass))
                        )
                        .discardingFiredPanes()
                )
                .apply(GroupByKey.create())
                .apply("Log", ParDo.of(new DoFn<KV<String, Iterable<String>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("\n TRIGGER " + c.element().getValue() + "\n" + c.pane());
                        c.output(c.pane().toString());
                    }
                }));


        p.run();
    }

}

        /*
        EXPLANATION

        The trigger `AfterWatermark.pastEndOfWindow()` is the default trigger when the window closes, but we added two
        conditions to trigger ONCE early and Late

        1 - The first trigger contains "first" to "third", which are the early firings because of
        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(delay), so all elements that arrived from 0 to 5.
        2 - Second trigger contains the rest of the elements that arrived on time before the window closed
        3 - From now on, we get the late elements with AfterPane.elementCountAtLeast(countPass), so it triggers every 3
        elements
        4 - "Late-7" is triggered alone due to exceeding the allowed lateness
         */