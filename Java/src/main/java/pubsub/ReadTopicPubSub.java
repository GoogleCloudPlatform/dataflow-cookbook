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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReadTopicPubSub {

    private static final Logger LOG = LoggerFactory.getLogger(ReadTopicPubSub.class);


    public interface ReadTopicPubSubOptions extends PipelineOptions {
        @Description("Topic to read from")
        @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {
        ReadTopicPubSubOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadTopicPubSubOptions.class);

        Pipeline p = Pipeline.create(options);

        p
                // Reading from a Topic creates a subscription to it automatically
                .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
                // Next ParDo is added so pipeline doesn't finish
                .apply("ParDo", ParDo.of(new DoFn<String, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                c.output(c.element());
                            }
                        })
                );

        p.run();
    }
}
