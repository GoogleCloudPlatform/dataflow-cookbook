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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.ProducerRecordCoder;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class WriteDynamicKafka {

    private static final Logger LOG = LoggerFactory.getLogger(WriteDynamicKafka.class);

    public interface WriteDynamicKafkaOptions extends DataflowPipelineOptions {
        @Description("Kafka Bootstrap")
        String getBootstrap();

        void setBootstrap(String value);
    }

    public static void main(String[] args) {
        WriteDynamicKafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteDynamicKafkaOptions.class);

        Pipeline p = Pipeline.create(options);

        final List<KV<Integer, String>> elements = Arrays.asList(
                KV.of(1, "this"),
                KV.of(3, "is"),
                KV.of(5, "a"),
                KV.of(7, "message"),
                KV.of(2, "Another"),
                KV.of(4, "message"),
                KV.of(6, "t2"),
                KV.of(8, "topic")
        );

        p
                .apply(Create.of(elements))
                .apply("Convert To Record", ParDo.of(new DoFn<KV<Integer, String>, ProducerRecord<Integer, String>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                // process topic
                                String topic = c.element().getKey() % 2 == 0 ? "t2" : "t1";
                                // topic, key, value
                                ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, c.element().getKey(), c.element().getValue());
                                c.output(record);
                            }
                        })
                ).setCoder(ProducerRecordCoder.of(VarIntCoder.of(), StringUtf8Coder.of())) // We need a coder
                // Write as records
                .apply(KafkaIO.<Integer, String>writeRecords()
                        .withBootstrapServers(options.getBootstrap())
                        .withValueSerializer(StringSerializer.class)
                        .withKeySerializer(IntegerSerializer.class)
                );


        p.run();
    }
}
