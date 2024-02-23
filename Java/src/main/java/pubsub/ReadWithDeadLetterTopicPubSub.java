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
package pubsub;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.nio.charset.StandardCharsets;

/**
 * Pipeline for reading messages from Google Cloud Pub/Sub, incorporating error handling through a
 * dead-letter topic. The pipeline uses the {@code PubsubIO.readStrings()} transform to read
 * messages from a specified subscription, and any parsing errors are captured and redirected to a
 * dead-letter topic.
 */
public class ReadWithDeadLetterTopicPubSub {

    public static final String ID_EXAMPLE_SUFFIX = "id=";

    /** Pipeline options for read from Google Cloud Pub/Sub. */
    public interface ReadPubSubOptions extends PipelineOptions {
        @Description("Google Cloud Pub/Sub subscription to read from")
        @Validation.Required
        String getSubscription();

        void setSubscription(String subscription);

        @Description("Google Cloud Pub/Sub dead letter topic to write errors to")
        @Validation.Required
        String getDeadLetterTopic();

        void setDeadLetterTopic(String deadLetterTopic);
    }

    public static void main(String[] args) {
        ReadPubSubOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadPubSubOptions.class);

        Pipeline p = Pipeline.create(options);

        p.apply(
            "Read from Pub/Sub",
            PubsubIO.readStrings()
                .fromSubscription(options.getSubscription())
                .withDeadLetterTopic(options.getDeadLetterTopic())
                .withCoderAndParseFn(
                    StringUtf8Coder.of(),
                    SimpleFunction.fromSerializableFunctionWithOutputType(
                        message -> {
                            String str = new String(message.getPayload(), StandardCharsets.UTF_8);

                            //Example check if it is correct string
                            if (!str.contains(ID_EXAMPLE_SUFFIX)) {
                                throw new RuntimeException("This message will be delivered to dead letter topic");
                            }
                            return str;
                        },
                        TypeDescriptors.strings())));
        p.run();
    }
}
