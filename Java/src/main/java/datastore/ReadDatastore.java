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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ReadDatastore {
    /*
    TODO

    This pipeline needs a Datastore instance, in this example we use
    quickstart examples:
    https://cloud.google.com/datastore/docs/quickstart
    */

    private static final Logger LOG = LoggerFactory.getLogger(ReadDatastore.class);

    public interface ReadDatastoreOptions extends DataflowPipelineOptions {
        @Description("Query to run")
        @Default.String("SELECT * FROM Task")
        String getQuery();

        void setQuery(String value);

        @Nullable
        @Description("Project ID")
        String getProjectDS();

        void setProjectDS(String value);
    }

    public static void main(String[] args) {
        ReadDatastoreOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadDatastoreOptions.class);

        Pipeline p = Pipeline.create(options);

        // Use Dataflow project if ProjectDS not passed
        String project = (options.getProjectDS() == null) ? options.getProject() : options.getProjectDS();

        p
                .apply(DatastoreIO.v1().read()
                        .withLiteralGqlQuery(options.getQuery())
                        .withProjectId(project)
                )
                .apply("Process Entity", ParDo.of(new DoFn<Entity, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                Entity entity = c.element();
                                com.google.datastore.v1.Key dsKey = entity.getKey();

                                String namespace = dsKey.getPartitionId().getNamespaceId();
                                String entryString = "Namespace: " + namespace + "\n";

                                Map<String, Value> properties = entity.getPropertiesMap();

                                for (Map.Entry<String, Value> entry : properties.entrySet()) {
                                    entryString = String.format("%s \t property: %s, value: %s",
                                            entryString, entry.getKey(), entry.getValue());
                                }

                                LOG.info(entryString);
                                c.output(entryString);
                            }
                        })
                );

        p.run();
    }
}
