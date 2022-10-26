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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionWindow {
    private static final Logger LOG = LoggerFactory.getLogger(SessionWindow.class);

    public interface SessionWindowOptions extends DataflowPipelineOptions {
        @Description("Topic to read from")
        @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {
        SessionWindowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SessionWindowOptions.class);

        Pipeline p = Pipeline.create(options);

        Duration windowGap = Duration.standardSeconds(120);

        p
                .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
                //Convert to Row
                .apply("Convert To KV", ParDo.of(new DoFn<String, KV<String, Double>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                JSONObject json = new JSONObject(c.element());

                                String rideId = json.getString("ride_id"); // this is the session
                                Double meterIncrement = json.getDouble("meter_increment");

                                String rideStatus = json.getString("ride_status");

                                if (rideStatus.equals("enroute")) {
                                    c.output(KV.of(rideId, meterIncrement));
                                }
                            }
                        })
                )
                .apply(Window.<KV<String, Double>>into(
                        Sessions.withGapDuration(windowGap)) // if no new elements with the same key appear within gap time, close window
                        .withAllowedLateness(Duration.standardSeconds(10))
                        .discardingFiredPanes()
                )
                .apply(Sum.doublesPerKey())
                .apply("Log", ParDo.of(new DoFn<KV<String, Double>, String>() {
                        @ProcessElement
                        public void processElement(@Element KV<String, Double> e, OutputReceiver<String> out,
                                                   BoundedWindow b) {
                            String log = String.format("Ride id %s costed %f$ for window %s",
                                   e.getKey(), e.getValue(), b);
                            LOG.info(log);
                            out.output(log);
                        }
                    })
                );

        p.run();
    }
}
