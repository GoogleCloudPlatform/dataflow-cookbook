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
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class WriteWithAttributesPubSub {


    private static final Logger LOG = LoggerFactory.getLogger(WriteWithAttributesPubSub.class);

    /**
     * Pipeline options to be passed via the command line.
     */
    public interface WriteWithAttributesPubSubOptions extends PipelineOptions {
        @Description("Topic to write to")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {

        // Parse pipeline options from the command line.
        WriteWithAttributesPubSubOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteWithAttributesPubSubOptions.class);

        // The pipeline will write the following hard-coded records.
        // This is going to be a batch pipeline but writing in Streaming works the same.
        final List<String> elements = Arrays.asList(
                //Name, Product, Epoch time millis
                "Robert, TV, 1613141590000",
                "Maria, Phone, 1612718280000",
                "Juan, Laptop, 1611618000000",
                "Rebeca, Videogame, 1610000000000"
        );

        // Create the pipeline.
        Pipeline p = Pipeline.create(options);

        p
                // Add a source that uses the hard-coded records.
                .apply(Create.of(elements))
                // Add a transform that converts string into Pub/Sub messages with attributes.
                .apply("to PubSubMessage", ParDo.of(new DoFn<String, PubsubMessage>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] columns = c.element().split(", ");

                        HashMap<String, String> attributes = new HashMap<String, String>();
                        attributes.put("timestamp", columns[2]);
                        attributes.put("buyer", columns[0]);

                        PubsubMessage message = new PubsubMessage(c.element().getBytes(StandardCharsets.UTF_8), attributes);

                        c.output(message);
                    }
                }))
                // Add a sink that writes messages to Pub/Sub.
                .apply(PubsubIO.writeMessages().to(options.getTopic()));

        // Execute the pipeline.
        p.run();
    }
}
