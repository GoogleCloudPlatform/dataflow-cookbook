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
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class WriteStringPubSub {

    private static final Logger LOG = LoggerFactory.getLogger(WriteStringPubSub.class);

    public interface WriteStringPubSubOptions extends PipelineOptions {
        @Description("Topic to write to")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {

        WriteStringPubSubOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteStringPubSubOptions.class);

        Pipeline p = Pipeline.create(options);

        final List<String> elements = Arrays.asList(
                "Message",
                "Another message",
                "Yet another message"
        );

        p
                .apply(Create.of(elements))
                .apply(PubsubIO.writeStrings().to(options.getTopic())); // Writing in Streaming works the same

        p.run();
    }

}
