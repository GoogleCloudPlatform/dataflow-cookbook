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

public class AfterEachTrigger {
    private static final Logger LOG = LoggerFactory.getLogger(AfterEachTrigger.class);

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
                .advanceProcessingTime(Duration.millis(12000))
                .addElements(
                        TimestampedValue.of("third", new Instant(2500))
                )
                .advanceWatermarkTo(new Instant(3000))
                .advanceProcessingTime(Duration.millis(500))
                .addElements(
                        TimestampedValue.of("fourth", new Instant(2000))
                )
                .advanceWatermarkTo(new Instant(3500))
                .advanceProcessingTime(Duration.millis(500))
                .addElements(
                        TimestampedValue.of("fifth", new Instant(3500)),
                        TimestampedValue.of("sixth", new Instant(3500))

                )
                .advanceWatermarkTo(new Instant(4000))
                .advanceProcessingTime(Duration.millis(500))
                .addElements(
                        TimestampedValue.of("seventh", new Instant(5000))
                )
                .advanceWatermarkTo(new Instant(6000))
                .advanceProcessingTime(Duration.millis(2000))
                .addElements(
                        TimestampedValue.of("eighth", new Instant(5000))
                )
                .advanceWatermarkTo(new Instant(7000))
                .advanceProcessingTime(Duration.millis(1000))
                .addElements(
                        TimestampedValue.of("ninth", new Instant(5000)),
                        TimestampedValue.of("tenth", new Instant(5000))
                )
                .advanceWatermarkTo(new Instant(8000))
                .advanceProcessingTime(Duration.millis(1000))
                .addElements(
                        TimestampedValue.of("eleventh", new Instant(5000))
                )
                .advanceWatermarkTo(new Instant(9000))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(
                        TimestampedValue.of("last-in-window", new Instant(8000))
                )
                .advanceWatermarkTo(new Instant(10500))
                .advanceProcessingTime(Duration.millis(1500))
                .addElements(
                        TimestampedValue.of("Late-1", new Instant(0))
                )
                .advanceWatermarkTo(new Instant(11500))
                .advanceProcessingTime(Duration.millis(1000))
                .addElements(
                        TimestampedValue.of("Late-2", new Instant(0)),
                        TimestampedValue.of("Late-3", new Instant(8000))

                )
                .advanceWatermarkTo(new Instant(12000))
                .advanceProcessingTime(Duration.millis(500))
                .addElements(
                        TimestampedValue.of("Late-4", new Instant(7000)),
                        TimestampedValue.of("Late-5", new Instant(8000))
                )
                .advanceWatermarkTo(new Instant(14500))
                .advanceProcessingTime(Duration.millis(2500))
                .addElements(
                        TimestampedValue.of("Late-6", new Instant(8000))
                )
                .advanceWatermarkTo(new Instant(16000))
                .advanceProcessingTime(Duration.millis(1500))
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
                                                AfterEach.inOrder(
                                                        AfterPane.elementCountAtLeast(countPass), // trigger if count >= 3
                                                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(delay), // trigger if delay > 2s
                                                        AfterPane.elementCountAtLeast(1) // trigger if count >= 1
                                                        , AfterWatermark.pastEndOfWindow() // try commenting this line!
//                                                ,AfterPane.elementCountAtLeast(1) // try uncommenting this line!
                                                )
                                                // orFinally stops the repetition of AfterEach
//                                                        .orFinally(AfterWatermark.pastEndOfWindow()) // try uncommenting this line!
                                        )
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

        AfterEach triggers in order of the triggers that are within it.

        1 - The first trigger contains "first" to "third" even if "third" arrived 2.5 later than first. It was triggered
        due to AfterPane.elementCountAtLeast(countPass) being the first trigger
        2 - Second trigger contains "fourth" to "seventh", which are the elements within 2s of "fourth" (from 3 to 5). In
        this case the trigger was "AfterProcessingTime.pastFirstElementInPane().plusDelayOf(delay)"
        3 - "eighth" appears by itself due to AfterPane.elementCountAtLeast(1)
        4 - "ninth" to "eleventh" and "last-in-window" are triggered since the window closed, and we have trigger
        "AfterWatermark.pastEndOfWindow()".
        5 - "Late-(1 to 3)" are put together because of the count 3 of elements, we are repeating the whole AfterEach
        6 - "Late-4,5" appears by themselves since they were triggered by AfterProcessingTime.pastFirstElementInPane().plusDelayOf(delay)
        7 - "Late-6 is by itself due to "AfterPane.elementCountAtLeast(1)"

        If we comment "AfterWatermark.pastEndOfWindow()" we would NOT get "ninth", "tenth", "eleventh" together with
        "last-in-window", since we are repeating through AfterEach, so "AfterPane.elementCountAtLeast(countPass)" is the trigger.
        "last-in-window" would be with "Late-1" due to "AfterProcessingTime.pastFirstElementInPane().plusDelayOf(delay)"

        If we uncomment ",AfterPane.elementCountAtLeast(1)", "Late-1" would be triggered alone (due to count >=1) and then we
        repeat the AfterEach

        If we uncomment ".orFinally(AfterWatermark.pastEndOfWindow())" after the window closes we stop repeating. This
        means that the "Late-X" elements will be triggered as they come.
        */
