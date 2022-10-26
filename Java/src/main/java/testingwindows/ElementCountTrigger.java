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

public class ElementCountTrigger {
    private static final Logger LOG = LoggerFactory.getLogger(ElementCountTrigger.class);

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        Integer windowLength = 10;
        Integer allowLateSize = 5;
        Integer countElements = 2; // Trigger if 2 elements enter the window

        // Behaviour is explained in detail at the end of the file
        // Note: all elements would fall into the same window
        TestStream<String> streamEvents = TestStream.create(StringUtf8Coder.of())
                .addElements(
                        TimestampedValue.of("first", new Instant(0))
                )
                .advanceWatermarkTo(new Instant(2000))// move Watermark time to sec 2
                .advanceProcessingTime(Duration.standardSeconds(2)) // move Processing time to sec 2
                .addElements(
                        TimestampedValue.of("second", new Instant(1000))
                )
                .advanceWatermarkTo(new Instant(4000))// move Watermark time to sec 4
                .advanceProcessingTime(Duration.standardSeconds(2)) // move Processing time to sec 4
                .addElements(
                        TimestampedValue.of("third", new Instant(3000))
                )
                .advanceWatermarkTo(new Instant(6000)) // move Watermark time to sec 6
                .advanceProcessingTime(Duration.standardSeconds(2)) // move Processing time to sec 6
                .addElements(
                        // These elements arrive at the same processing time
                        TimestampedValue.of("same-time-1", new Instant(4000)),
                        TimestampedValue.of("same-time-2", new Instant(5000)),
                        TimestampedValue.of("same-time-3", new Instant(6000))
                )
                .advanceWatermarkTo(new Instant(9000))
                .advanceProcessingTime(Duration.standardSeconds(3))
                .addElements(
                        // These elements arrive at the same time
                        TimestampedValue.of("last-in-window", new Instant(8000))
                )
                .advanceWatermarkTo(new Instant(11000))
                .advanceProcessingTime(Duration.standardSeconds(2))
                .addElements(
                        // element arrives at sec 11 but fall in the window
                        TimestampedValue.of("Late-1", new Instant(0)),
                        TimestampedValue.of("Late-2", new Instant(5000)),
                        TimestampedValue.of("Late-3", new Instant(5550))
                )
                .advanceWatermarkTo(new Instant(12000))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(
                        TimestampedValue.of("Late-4", new Instant(7000))

                )
                .advanceWatermarkTo(new Instant(13000))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(
                        TimestampedValue.of("Late-5", new Instant(8000))

                )
                .advanceWatermarkTo(new Instant(14000))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(
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
                        .triggering(
                                // If we dont trigger Repeatedly, after the first trigger, the rest of the elements would be discarded
                                // https://s.apache.org/finishing-triggers-drop-data
                                Repeatedly.forever(
                                        AfterPane.elementCountAtLeast(countElements)
                                                // orFinally stops the repetition of AfterPane
                                                .orFinally(AfterWatermark.pastEndOfWindow())  // Try the pipeline commenting this line!
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

        The idea of the trigger is to fire every two elements, but there are some constrains. Let's check the output

        1 - The first trigger contains "first" and "second", which is expected since they are the first 2 elements.
        2 - The second trigger contains the third element but all "same-time-x" elements, so it is not just 2 elements.
        This happens because of "same-time-x" arrive at the same time, so there's not second element but second elements
        3 - The third trigger only contains "last-in-window", this is because we have `.orFinally(AfterWatermark.pastEndOfWindow())`
        so it triggers when the window closes, no matter the elements there are.
        4 - The fourth trigger contains "Late-(1,2,3)", since they arrive together
        5 - The next triggers contain the elements as they come, since we have `.orFinally(AfterWatermark.pastEndOfWindow())`
        6 - Element "Outside-allowed-lateness" is discarded as it's outside the allowed lateness

        If we comment ".orFinally(AfterWatermark.pastEndOfWindow())" triggers (3) and (4) are put together, since (3) does not
        contain 2 elements. The next triggers follow the 2 elements rule, so "late-4" and "late-5" are put together, but
        "late-6" is by itself since there are no more elements and the allowed late time passed.

        In a real life scenario, it's not that common two elements arrive at the exact same time.
         */