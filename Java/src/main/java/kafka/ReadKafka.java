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
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadKafka {

    private static final Logger LOG = LoggerFactory.getLogger(ReadKafka.class);

    public interface ReadKafkaOptions extends DataflowPipelineOptions {
        @Description("Kafka Bootstrap")
        String getBootstrap();

        void setBootstrap(String value);

        @Description("Kafka Topic")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {
        ReadKafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadKafkaOptions.class);

        Pipeline p = Pipeline.create(options);

        p
                .apply(KafkaIO.<Integer, String>read()
                        .withBootstrapServers(options.getBootstrap())
                        .withTopic(options.getTopic())
                        .withKeyDeserializer(IntegerDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                )
                .apply("Log", ParDo.of(new DoFn<KafkaRecord<Integer, String>, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                KV<Integer, String> kv = c.element().getKV();
                                String message = String.format("Key: %s, Value: %s",
                                        kv.getKey(), kv.getValue());
                                LOG.info(message);
                                c.output(message);
                            }
                        })
                );

        p.run();
    }
}
