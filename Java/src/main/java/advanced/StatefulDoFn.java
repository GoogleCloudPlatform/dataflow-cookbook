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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Instant;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;

public class StatefulDoFn {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulDoFn.class);

    public interface StatefulDoFnOptions extends DataflowPipelineOptions {

        @Description("Topic to read from")
        @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {
        StatefulDoFnOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(StatefulDoFnOptions.class);

        Pipeline p = Pipeline.create(options);

        p
                .apply("Read From PubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
                .apply("Parse and to KV", ParDo.of(new DoFn<String, KV<String, Integer>>() {
                                                       @ProcessElement
                                                       public void processElement(ProcessContext c) throws ParseException {
                                                           JSONObject json = new JSONObject(c.element());
                                                           String rideStatus = json.getString("ride_status");
                                                           Integer passengerCount = json.getInt("passenger_count");

                                                           c.output(KV.of(rideStatus, passengerCount));
                                                       }
                                                   }
                ))
                // Stateful DoFn need to have a KV as input
                .apply("Stateful DoFn", ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
                            // GroupIntoBatches PTransfrom implements a similar logic.
                            // When using StatefulDoFns be careful of not keeping the state forever and clearing it.
                            // This example is OK because we know the keys are always incoming, but if we have
                            // sparse keys, we may keep the buffer up forever (e.g., we trigger every 100 elements
                            // but we only got 99 for that key). Adding a Timer would fix that, see TimerDoFn
                            // for an example.
                            private static final int MAX_BUFFER_SIZE_ENROUTE = 100000;
                            private static final int MAX_BUFFER_SIZE = 100; // Special counter for non-enroutes

                            @StateId("buffer")
                            // Elements will be saved here, with type KV<String,Integer>
                            private final StateSpec<BagState<KV<String, Integer>>> bufferedEvents = StateSpecs.bag();

                            @StateId("count")
                            // Count of elements in buffer
                            private final StateSpec<ValueState<Integer>> countState = StateSpecs.value();

                            @ProcessElement
                            public void processElement(ProcessContext c,
                                                       @StateId("buffer") BagState<KV<String, Integer>> bufferState,
                                                       @StateId("count") ValueState<Integer> countState)
                                    throws ParseException {

                                // Note: States are tied to the key and window, so each key/window would have its own bag and counter
                                int count = firstNonNull(countState.read(), 0);
                                count = count + 1;
                                countState.write(count); // overwrite counter
                                bufferState.add(c.element()); // add to buffer the element

                                // Output if count > 100000 and key is enroute, or if count > 10 otherwise.
                                Boolean enroute = (count >= MAX_BUFFER_SIZE_ENROUTE && c.element().getKey().equals("enroute"));
                                Boolean notEnroute = (count >= MAX_BUFFER_SIZE && !c.element().getKey().equals("enroute"));
                                if (enroute || notEnroute) {
                                    LOG.info("Clearing buffer at " + Instant.now().toString() +
                                            ", total of " + countState.read()
                                            + " elements with key " + c.element().getKey());
                                    for (KV<String, Integer> passengerKV : bufferState.read()) { // for every element in buffer
                                        c.output(passengerKV);
                                    }
                                    // Clearing buffer and counter
                                    bufferState.clear();
                                    countState.clear();
                                }
                            }
                        })
                );

        p.run();

    }
}
