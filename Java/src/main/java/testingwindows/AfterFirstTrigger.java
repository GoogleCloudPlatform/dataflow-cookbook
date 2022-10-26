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

public class AfterFirstTrigger {

    private static final Logger LOG = LoggerFactory.getLogger(ElementCountTrigger.class);

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        Integer windowLength = 10;
        Integer allowLateSize = 5;
        Integer countPass = 3; // Trigger if 3 elements pass the window
        Duration delay = Duration.standardSeconds(2);

        // Behaviour is explained in detail at the end of the file
        // Note: all elements would fall into the same window
        TestStream<String> streamEvents = TestStream.create(StringUtf8Coder.of())
                .addElements(
                        TimestampedValue.of("first", new Instant(0))
                )
                .advanceWatermarkTo(new Instant(1500))
                .advanceProcessingTime(Duration.millis(1500))
                .addElements(
                        TimestampedValue.of("second", new Instant(1000))
                )
                .advanceWatermarkTo(new Instant(2500))
                .advanceProcessingTime(Duration.millis(1000))
                .addElements(
                        TimestampedValue.of("third", new Instant(3000))
                )
                .advanceWatermarkTo(new Instant(3000))
                .advanceProcessingTime(Duration.millis(500))
                .addElements(
                        TimestampedValue.of("fourth", new Instant(4000))
                )
                .advanceWatermarkTo(new Instant(3500))
                .advanceProcessingTime(Duration.millis(500))
                .addElements(
                        TimestampedValue.of("fifth", new Instant(5000))
                )
                .advanceWatermarkTo(new Instant(4000))
                .advanceProcessingTime(Duration.millis(500))
                .addElements(
                        TimestampedValue.of("sixth", new Instant(5000))
                )
                .advanceWatermarkTo(new Instant(9000))
                .advanceProcessingTime(Duration.standardSeconds(5))
                .addElements(
                        // These elements arrive at the same time
                        TimestampedValue.of("last-in-window", new Instant(8000))
                )
                .advanceWatermarkTo(new Instant(10500))
                .advanceProcessingTime(Duration.millis(1500))
                .addElements(
                        // element arrives at sec 11 but fall in the window
                        TimestampedValue.of("Late-1", new Instant(0))
                )
                .advanceWatermarkTo(new Instant(11500))
                .advanceProcessingTime(Duration.millis(1000))
                .addElements(
                        TimestampedValue.of("Late-2", new Instant(7000)),
                        TimestampedValue.of("Late-3", new Instant(8000)),
                        TimestampedValue.of("Late-4", new Instant(8000))
                )
                .advanceWatermarkTo(new Instant(12000))
                .advanceProcessingTime(Duration.millis(500))
                .addElements(
                        TimestampedValue.of("Late-5", new Instant(8000))
                )
                .advanceWatermarkTo(new Instant(16000))
                .advanceProcessingTime(Duration.standardSeconds(2))
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
                                // If we dont trigger Repeatedly, after the first trigger, the rest of the elements would be discarded
                                // https://s.apache.org/finishing-triggers-drop-data
                                Repeatedly.forever(
                                        AfterFirst.of(
                                                AfterPane.elementCountAtLeast(countPass), // trigger if count >= 3
                                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(delay) // trigger if delay > 2s
                                        )
                                )
                        )
                        .discardingFiredPanes()
                )
                .apply(GroupByKey.create())
                .apply("Log", ParDo.of(new DoFn<KV<String, Iterable<String>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("\n TRIGGER " + c.element().getValue().toString());
                        c.output(c.pane().toString());
                    }
                }))
        ;


        p.run();
    }
}
        /*
        EXPLANATION

        The trigger AfterAny means that once any of the triggers within it happen, we get the elements.
        There are two triggers within this code's AfterAny, one that triggers after we have 3 elements and other
        when 2 seconds have passed from the first element we encountered

        1 - The first trigger contains "first" (arriving at processing time 0) and "second", because the processing time
        advanced past second 2 , so the `AfterProcessingTime.pastFirstElementInPane().plusDelayOf(delay)` triggered with 2 elements,
        since first element arrived at 0 and `delay` is 2.
        2 - Second trigger contains "third" (arriving at 2.5) to "fifth", but not "sixth" even if it arrived at processing
        time 4 (within the 2s delay that goes from 2.5 to 4.5). This is becuase the pane contained 3 elements and
        `AfterPane.elementCountAtLeast(countPass)` triggered
        3 - "sixth" appear by its own, since no more elements appear within the delay
        4 - "last-in-window" and "Late-1" were triggered together at 11, but after window closing at 11. This is because
        we are using the delay from "last-in-window" which is from 9 to 11
        5 - "Late-(2 to 4)" are put together because of the count 3 of elements, even if "Late-5" arrived within the delay
        6 - "Late-5" appears by itself since it was triggered by AfterProcessingTime.pastFirstElementInPane().plusDelayOf(delay)
        */