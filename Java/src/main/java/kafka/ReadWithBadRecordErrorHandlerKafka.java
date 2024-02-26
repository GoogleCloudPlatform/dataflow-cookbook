/*
 * Copyright 2024 Google LLC
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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.BadRecordErrorHandler;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Charsets;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline for reading from Kafka with a custom error handling strategy for bad records. The
 * pipeline reads messages from a specified Kafka topic, deserializes them using a custom
 * deserializer, and logs the key-value pairs. It utilizes a custom error handler for handling bad
 * records and counting the number of such records using a custom transform.
 */
public class ReadWithBadRecordErrorHandlerKafka {

    private static final Logger LOG =
            LoggerFactory.getLogger(ReadWithBadRecordErrorHandlerKafka.class);

    /**
     * Pipeline options for read from Kafka.
     */
    public interface ReadKafkaOptions extends PipelineOptions {

        @Description("Apache Kafka bootstrap servers")
        @Validation.Required
        String getBootstrapServers();

        void setBootstrapServers(String bootstrapServers);
        
        @Description("Apache Kafka topic to read from")
        @Validation.Required
        String getTopic();

        void setTopic(String topic);
    }

    public static void main(String[] args) {
        ReadKafkaOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation().as(ReadKafkaOptions.class);

        Pipeline p = Pipeline.create(options);

        BadRecordErrorHandler<PCollection<Long>> errorHandler =
                p.registerBadRecordErrorHandler(new ErrorSinkTransform());
        
        p.apply(
                "Read from Kafka",
                KafkaIO.<String, String>read()
                    .withBootstrapServers(options.getBootstrapServers())
                    .withTopic(options.getTopic())
                    .withKeyDeserializer(ExampleDeserializer.class)
                    .withValueDeserializer(ExampleDeserializer.class)
                    .withBadRecordErrorHandler(errorHandler))
            .apply(
                "Log Data",
                ParDo.of(
                    new DoFn<KafkaRecord<String, String>, KafkaRecord<String, String>>() {
                        @DoFn.ProcessElement
                        public void processElement(ProcessContext c) {
                            LOG.info(
                                "Key = {}, Value = {}",
                                c.element().getKV().getKey(),
                                c.element().getKV().getValue());
                            c.output(c.element());
                        }
                    }));
        p.run();
    }

    /**
     * Custom transform to process a {@link PCollection} of {@link BadRecord}. It applies a global
     * combine operation using {@link Count} combine function to count the total number of {@link
     * BadRecord} instances in the input {@link PCollection}.
     */
    public static class ErrorSinkTransform extends PTransform<PCollection<BadRecord>, PCollection<Long>> {
        
        @UnknownKeyFor
        @NonNull
        @Initialized
        public PCollection<Long> expand(PCollection<BadRecord> input) {
            return input.apply("Combine", Combine.globally(Count.combineFn()));
        }
    }

    /**
     * Custom deserializer for Kafka messages. This implementation deserializes byte arrays into
     * strings. It checks for a predefined sequence in the deserialized string and throws a {@code
     * SerializationException} if the sequence is not found. The intention is to demonstrate handling
     * deserialization exceptions in a Kafka consumer.
     */
    public static class ExampleDeserializer implements Deserializer<String> {

        private static final CharSequence EXAMPLE_SEQUENCE = "kafka_";

        public ExampleDeserializer() {}

        @Override
        public String deserialize(String topic, byte[] data) {

            if (data == null) {
                return null;
            }
            String str = new String(data, Charsets.UTF_8);

            //Example check
            if (!str.contains(EXAMPLE_SEQUENCE)) {
                throw new SerializationException("Intentional serialization exception");
            }
            return str;
        }
    }
}
