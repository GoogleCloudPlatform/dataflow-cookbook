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

package pubsublite;

import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteIO;
import org.apache.beam.sdk.io.gcp.pubsublite.SubscriberOptions;
import org.apache.beam.sdk.io.gcp.pubsublite.UuidDeduplicationOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ReadPubSubLite {


    private static final Logger LOG = LoggerFactory.getLogger(ReadPubSubLite.class);

    public interface ReadPubSubLiteOptions extends PipelineOptions {
        @Description("Subscription to read from")
        String getSubscription();

        void setSubscription(String value);

        @Description("Boolean to deduplicate")
        @Default.Boolean(false)
        Boolean getDeduplication();

        void setDeduplication(Boolean value);
    }

    public static void main(String[] args) {
        ReadPubSubLiteOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadPubSubLiteOptions.class);

        Pipeline p = Pipeline.create(options);

        // Get Lite subscription
        SubscriberOptions subscriberOptions =
                SubscriberOptions.newBuilder()
                        .setSubscriptionPath(SubscriptionPath.parse(options.getSubscription()))
                        .build();

        PCollection<SequencedMessage> messages = p
                .apply("ReadFromPubSubLite", PubsubLiteIO.read(subscriberOptions));

        // Optionally deduplicate messages. It requires UUIDs.
        // https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/pubsublite/PubsubLiteIO.html#deduplicate-org.apache.beam.sdk.io.gcp.pubsublite.UuidDeduplicationOptions-
        if (options.getDeduplication()) {
            messages = messages
                    .apply(PubsubLiteIO.deduplicate(UuidDeduplicationOptions.newBuilder().build()));
        }

        messages
                .apply("Log Message", ParDo.of(new DoFn<SequencedMessage, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                SequencedMessage message = c.element();
                                String data = message.getMessage().getData().toStringUtf8();
                                Long timestampSeconds = message.getPublishTime().getSeconds();
                                DateTimeFormatter format = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");
                                String timestamp = Instant.ofEpochSecond(timestampSeconds).toString(format);

                                // Optionally go through the attributes
                                Map<String, AttributeValues> attributes = message.getMessage().getAttributesMap();

                                String fullMessage = String.format("Data '%s' published at %s. \n\t Attributes %s", data, timestamp, attributes);
                                LOG.info(fullMessage);
                                c.output(fullMessage);
                            }
                        })
                );

        p.run();
    }
}
