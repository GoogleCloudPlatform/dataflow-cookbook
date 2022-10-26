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

package kafka;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

public class WriteStreamingKafka {

    private static final Logger LOG = LoggerFactory.getLogger(WriteStreamingKafka.class);

    public interface WriteKafkaOptions extends DataflowPipelineOptions {
        @Description("Kafka Bootstrap")
        String getBootstrap();

        void setBootstrap(String value);

        @Description("Kafka Topic")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {
        WriteKafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteKafkaOptions.class);

        Pipeline p = Pipeline.create(options);

        String topic = "projects/pubsub-public-data/topics/taxirides-realtime";

        p
                .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(topic))
                .apply("Convert To KV", ParDo.of(new DoFn<String, KV<Integer, String>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws ParseException {
                                JSONObject json = new JSONObject(c.element());

                                String rideStatus = json.getString("ride_status");
                                Integer passengerCount = json.getInt("passenger_count");
                                String rideId = json.getString("ride_id");

                                if (rideStatus.equals("dropoff")) {
                                    c.output(KV.of(passengerCount, rideId));
                                }
                            }
                        })
                )
                .apply(KafkaIO.<Integer, String>write()
                        .withBootstrapServers(options.getBootstrap())
                        .withTopic(options.getTopic())
                        .withValueSerializer(StringSerializer.class)
                        .withKeySerializer(IntegerSerializer.class)
                        .withInputTimestamp()
                );


        p.run();
    }
}
