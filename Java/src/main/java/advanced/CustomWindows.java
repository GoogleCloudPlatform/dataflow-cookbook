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

package advanced;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

public class CustomWindows {

    private static final Logger LOG = LoggerFactory.getLogger(CustomWindows.class);


    public interface CustomWindowsOptions extends PipelineOptions {
        @Description("Topic to read from")
        @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        String getTopic();

        void setTopic(String value);
    }

    // Fixed window with size dependent on the element value
    public static class WindowByValueElement extends WindowFn<KV<String, Integer>, IntervalWindow> {

        @Override
        public Collection<IntervalWindow> assignWindows(AssignContext c) {
            Instant timestamp = c.timestamp();
            Integer passengers = c.element().getValue();

            // Assign window size depending on number passengers
            Duration size = Duration.standardMinutes(passengers * 10);

            // Define lower/upper bound
            Instant start = new Instant(
                    timestamp.getMillis() - timestamp.plus(size).getMillis() % size.getMillis());
            Instant end = start.plus(size);
            return Arrays.asList(new IntervalWindow(start, end));
        }

        // Mandatory methods
        @Override
        public void mergeWindows(MergeContext c) {
            // This defines what to do in case the windows need to be merged (not the case here)
            // Example: https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/windowing/MergeOverlappingIntervalWindows.java
        }

        @Override
        public boolean isCompatible(WindowFn<?, ?> other) {
            return this.equals(other); // equals defined bellow
        }

        @Override
        public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
            if (!this.isCompatible(other)) {
                throw new IncompatibleWindowException(
                        other,
                        String.format(
                                "Only %s objects with the same sizes and key to check are compatible.",
                                WindowByValueElement.class.getSimpleName()));
            }
        }


        @Override
        public boolean equals(@Nullable Object object) {
            return object instanceof WindowByValueElement;
        }


        @Override
        public Coder<IntervalWindow> windowCoder() {
            return IntervalWindow.getCoder();
        }

        @Override
        public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
            return null;
        }
    }


    public static void main(String[] args) {
        CustomWindowsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomWindowsOptions.class);

        Pipeline p = Pipeline.create(options);

        p
                .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
                .apply("to KV", ParDo.of(new DoFn<String, KV<String, Integer>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                JSONObject json = new JSONObject(c.element());

                                String rideId = json.getString("ride_id");
                                Integer passengerCount = json.getInt("passenger_count");

                                // Discard elements without passengers
                                if (passengerCount > 0) {
                                    c.output(KV.of(rideId, passengerCount));
                                }
                            }
                        })
                )
                .apply(Window.into(new WindowByValueElement()))
                .apply(Sample.fixedSizePerKey(1))
                .apply("log window size", ParDo.of(new DoFn<KV<String, Iterable<Integer>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c, BoundedWindow w) {
                        LOG.info("Element " + c.element().toString() +
                                " in window " + w.toString());
                        c.output(c.element().toString());
                    }
                }));

        p.run();
    }
}
