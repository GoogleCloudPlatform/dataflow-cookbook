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

package gcs;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

public class WriteStreamingTextIO {

    private static final Logger LOG = LoggerFactory.getLogger(WriteStreamingTextIO.class);


    public interface StreamingTextIOOptions extends PipelineOptions {
        @Description("Output to write to")
        String getOutput();

        void setOutput(String value);

        @Description("Num shards")
        @Default.Integer(10)
        Integer getNumShards();

        void setNumShards(Integer value);

        @Description("Topic to read from")
        @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {
        // Reference the extended class
        StreamingTextIOOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamingTextIOOptions.class);

        Pipeline p = Pipeline.create(options);

        Integer windowLength = 5;

        p
                .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
                .apply("Parse", ParDo.of(new DoFn<String, KV<String, Double>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws ParseException {
                                JSONObject json = new JSONObject(c.element());

                                String rideStatus = json.getString("ride_status");
                                Double passengerCount = json.getDouble("passenger_count");

                                c.output(KV.of(rideStatus, passengerCount));
                            }
                        })
                )
                .apply(Window.<KV<String, Double>>into(
                        FixedWindows.of(Duration.standardMinutes(windowLength)))
                        .withAllowedLateness(Duration.standardMinutes(windowLength))
                        .discardingFiredPanes())
                .apply(Sum.doublesPerKey())
                .apply("back to string", ParDo.of(new DoFn<KV<String, Double>, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws ParseException {
                                String now = Instant.now().toString();

                                String string = "At " + now + " the amount of passengers for the last " +
                                        windowLength + " minute(s) was " +
                                        c.element().getValue().toString() + " for type " + c.element().getKey();

                                c.output(string);
                            }
                        })
                )
                .apply("Write File(s)",
                        TextIO.write()
                                .withWindowedWrites()
                                .withNumShards(options.getNumShards())
                                .to(options.getOutput()))
        ;

        p.run();
    }
}
