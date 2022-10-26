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

package datastore;


import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;

public class WriteDatastore {
    /*
    TODO

    This pipeline needs a Datastore instance, in this example we use
    quickstart examples:
    https://cloud.google.com/datastore/docs/quickstart
    */

    private static final Logger LOG = LoggerFactory.getLogger(WriteDatastore.class);

    public interface WriteDatastoreOptions extends DataflowPipelineOptions {
        @Nullable
        @Description("Project ID")
        String getProjectDS();

        void setProjectDS(String value);
    }

    public static void main(String[] args) {
        WriteDatastoreOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteDatastoreOptions.class);

        Pipeline p = Pipeline.create(options);

        // Use Dataflow project if ProjectDS not passed
        String project = (options.getProjectDS() == null) ? options.getProject() : options.getProjectDS();

        final List<String> elements = Arrays.asList(
                "Anish, Smith, 1991",
                "Jenifer, Perez, 1973",
                "John, Johnson, 1929",
                "Kim, Chan, 2021"
        );

        p
                .apply(Create.of(elements))
                .apply("To Entity", ParDo.of(new DoFn<String, Entity>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                String[] row = c.element().split(", ");
                                Long year = Long.parseLong(row[2]);

                                // makeKey(KIND, ID)
                                com.google.datastore.v1.Key.Builder keyBuilder = makeKey("People", c.element().hashCode());
                                keyBuilder.getPartitionIdBuilder().setNamespaceId("Dataflow");

                                Entity entity = Entity.newBuilder()
                                        .setKey(keyBuilder)
                                        .putProperties("name", Value.newBuilder().setStringValue(row[0]).build())
                                        .putProperties("surname", Value.newBuilder().setStringValue(row[1]).build())
                                        .putProperties("year", Value.newBuilder().setDoubleValue(year).build())
                                        .build();

                                c.output(entity);
                            }
                        })
                )
                .apply(DatastoreIO.v1().write()
                        .withProjectId(project)
                );

        p.run();
    }
}
