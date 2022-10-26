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

package pubsub;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadWithAttributesPubSub {

    private static final Logger LOG = LoggerFactory.getLogger(ReadWithAttributesPubSub.class);


    public interface ReadWithAttributesPubSubOptions extends PipelineOptions {
        // You need a topic and messages with a timestamp attribute called "timestamp"
        // example https://stackoverflow.com/a/56129884/7650339
        // "timestamp" can be of format 1992-06-06T10:22:03.141Z or epoch in millis
        @Description("Topic to read from")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {
        ReadWithAttributesPubSubOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(ReadWithAttributesPubSubOptions.class);

        Pipeline p = Pipeline.create(options);

        p
                .apply("ReadFromPubSub", PubsubIO.readMessagesWithAttributes().fromTopic(options.getTopic())
                        .withTimestampAttribute("timestamp"))
                .apply("Log message", ParDo.of(new DoFn<PubsubMessage, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                PubsubMessage message = c.element();
                                // This log may overload the quota, comment if needed
                                String payload = new String(message.getPayload());
                                LOG.info("Element timestamp is " + c.timestamp().toString() +
                                        " and timestamp attribute is " + message.getAttribute("timestamp") + // both should match
                                        ". Message ID " + message.getMessageId() +
                                        ". Actual message \"" + payload + "\"");
                                c.output(payload);
                            }
                        })
                )
        ;

        p.run();
    }
}
