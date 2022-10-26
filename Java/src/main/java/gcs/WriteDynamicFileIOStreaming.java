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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Write.FileNaming;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;

import java.text.ParseException;

public class WriteDynamicFileIOStreaming {
    public interface writeDynamicFileIOStreamingOptions extends PipelineOptions {
        @Description("Output to write to")
        String getOutput();

        void setOutput(String value);

        @Description("Num shards")
        @Default.Integer(2)
        Integer getNumShards();

        void setNumShards(Integer value);

        @Description("Topic to read from")
        @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        String getTopic();

        void setTopic(String value);
    }

    public static String getStatusAndPassenger(KV<String, Integer> input) {
        String status = input.getKey();
        Integer passengers = input.getValue();

        if (passengers > 2) {
            return status + "-multiple";
        } else {
            return status + "-few";
        }
    }

    public static FileNaming fileWithDate(String prefix, String suffix) {
        return (window, pane, numShards, shardIndex, compression) -> {
            Instant windowClosure = window.maxTimestamp().plus(1);
            DateTimeFormatter dtf = DateTimeFormat.forPattern("YYYY/MM/dd/HH-mm--");
            String windowString = windowClosure.toString(dtf);

            String paneTiming =  pane.getTiming().toString().toLowerCase();

            return windowString + prefix + paneTiming + "-"
                    + shardIndex + "-of-" + numShards + suffix;
        };
    }


    public static void main(String[] args) {
        writeDynamicFileIOStreamingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(writeDynamicFileIOStreamingOptions.class);

        Pipeline p = Pipeline.create(options);

        Integer windowLength = 5;

        p
                .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
                .apply("Parse", ParDo.of(new DoFn<String, KV<String, Integer>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws ParseException {
                                JSONObject json = new JSONObject(c.element());

                                String rideStatus = json.getString("ride_status");
                                Integer passengerCount = json.getInt("passenger_count");

                                c.output(KV.of(rideStatus, passengerCount));
                            }
                        })
                )
                .apply(Window.<KV<String, Integer>>into(
                        FixedWindows.of(Duration.standardMinutes(windowLength)))
                        .withAllowedLateness(Duration.standardMinutes(windowLength))
                        .discardingFiredPanes())
                // The expected files would have path
                // options.getOutput()/<window-time-folder>-<getStatusAndPassenger>-<shard info>.txt
                // Coming from to .to/.withNaming.by. and the content of the file comes from .via
                .apply("Write File(s)",
                        FileIO.<String, KV<String, Integer>>writeDynamic()
                                .by(WriteDynamicFileIOStreaming::getStatusAndPassenger)
                                .withDestinationCoder(StringUtf8Coder.of())
                                .via(Contextful.fn(
                                        new SerializableFunction<KV<String, Integer>, String>() {
                                            @Override
                                            public String apply(KV<String, Integer> input) {
                                                return "Total passengers: " + input.getValue(); // this what we write
                                            }
                                        }),
                                        TextIO.sink())  // how we write it
                                .to(options.getOutput()) // path
                                .withNaming(key -> fileWithDate(key + "-", ".txt"))
                                .withNumShards(options.getNumShards())
                );

        p.run();

    }
}