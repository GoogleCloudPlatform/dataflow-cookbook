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
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

public class FixedWindow {

    private static final Logger LOG = LoggerFactory.getLogger(FixedWindow.class);

    public interface FixedWindowOptions extends DataflowPipelineOptions {
        @Description("Topic to read from")
        @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {
        FixedWindowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FixedWindowOptions.class);

        Pipeline p = Pipeline.create(options);

        Duration windowLength = Duration.standardMinutes(5);

        p
                .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
                //Convert to Row
                .apply("Convert To KV", ParDo.of(new DoFn<String, KV<String, Integer>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws ParseException {
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
                        FixedWindows.of(windowLength))
                        .withAllowedLateness(Duration.standardSeconds(60))
                        .discardingFiredPanes()
                )
                .apply(Sum.integersPerKey())
                .apply("Log", ParDo.of(new DoFn<KV<String, Integer>, String>() {
                        @ProcessElement
                        public void processElement(@Element KV<String, Integer> e, OutputReceiver<String> out,
                                                   BoundedWindow b) {
                            String log = String.format("A total of %d passengers have been %s for window %s",
                                    e.getValue(), e.getKey(), b);
                            LOG.info(log);
                            out.output(log);
                        }
                    })
                );

        p.run();
    }
}
