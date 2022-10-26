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

import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsublite.PublisherOptions;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class WritePubSubLite {


    private static final Logger LOG = LoggerFactory.getLogger(WritePubSubLite.class);


    public interface WritePubSubLiteOptions extends PipelineOptions {
        @Description("Topic to write to")
        String getTopic();

        void setTopic(String value);

        @Description("Boolean to add UUID")
        @Default.Boolean(true)
        Boolean getAddUUID();

        void setAddUUID(Boolean value);
    }

    public static void main(String[] args) {
        WritePubSubLiteOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WritePubSubLiteOptions.class);

        Pipeline p = Pipeline.create(options);

        final List<String> elements = Arrays.asList(
                "Message",
                "Another message",
                "Yet another message"
        );

        // Get Lite topic
        PublisherOptions publisherOptions =
                PublisherOptions.newBuilder()
                        .setTopicPath(TopicPath.parse(options.getTopic()))
                        .build();

        PCollection<PubSubMessage> messages = p
                .apply(Create.of(elements))
                .apply("to PubSubMessage", ParDo.of(new DoFn<String, PubSubMessage>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        ByteString data = ByteString.copyFromUtf8(c.element());

                        // These are optional
                        Instant now = Instant.now();
                        com.google.protobuf.Timestamp timestamp =
                                com.google.protobuf.Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build();
                        AttributeValues value = AttributeValues.newBuilder()
                                .addValues(ByteString.copyFromUtf8("dataflow"))
                                .build();

                        PubSubMessage message = PubSubMessage.newBuilder()
                                .setData(data)
                                .setEventTime(timestamp) // optional
                                .putAttributes("publisher", value) //optional
                                .build();

                        LOG.info(message.toString());

                        c.output(message);
                    }
                }));

        // Optionally add UUID
        if (options.getAddUUID()) {
            messages = messages
                    .apply("Add UUIDs", PubsubLiteIO.addUuids());
        }

        messages
                .apply("WritePubSubLite", PubsubLiteIO.write(publisherOptions));


        p.run();
    }
}
