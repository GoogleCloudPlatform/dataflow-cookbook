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

package windows;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalWindow {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalWindow.class);

    public interface GlobalWindowOptions extends PipelineOptions {
        @Description("Topic to read from")
        @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {
        GlobalWindowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GlobalWindowOptions.class);

        Pipeline p = Pipeline.create(options);

        // When using GlobalWindows be careful of not keeping the state forever and clearing state. In this example we
        // add a compound trigger so that we clean up state and don't let any window without triggering (e.g.,
        // we trigger every 100 elements but we only got 99 for that key). Check `testingWindows` folder for more info.
        Trigger trigger = Repeatedly.forever(
                AfterFirst.of(
                        AfterPane.elementCountAtLeast(100), // trigger if count >= 100
                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(60)) // trigger if delay > 60s
                ));

        p
                .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
                //Convert to Row
                .apply("Convert To KV", ParDo.of(new DoFn<String, KV<String, Integer>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                JSONObject json = new JSONObject(c.element());

                                String rideStatus = json.getString("ride_status");
                                Integer passengerCount = json.getInt("passenger_count");

                                if (!rideStatus.equals("enroute")) {
                                    c.output(KV.of(rideStatus, passengerCount));
                                }
                            }
                        })
                )
                .apply(Window.<KV<String, Integer>>into(
                        new GlobalWindows())
                        // we need a trigger so elements can be aggregated
                        .triggering(trigger)
                        .discardingFiredPanes()
                )
                .apply(Count.perKey())
                .apply("Log", ParDo.of(new DoFn<KV<String, Long>, String>() {
                            @ProcessElement
                            public void processElement(@Element KV<String, Long> e, OutputReceiver<String> out) {
                                String log = String.format("A total trips of %d have been %s.",
                                        e.getValue(), e.getKey());
                                LOG.info(log);
                                out.output(log);
                            }
                        })
                );

        p.run();
    }
}
