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

package spanner;

import com.google.cloud.spanner.Struct;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadQuerySpanner {
    /*
    TODO

    This pipeline needs a Spanner instance, in this example we use
    quickstart examples:
    https://cloud.google.com/spanner/docs/quickstart-console
    */

    private static final Logger LOG = LoggerFactory.getLogger(ReadQuerySpanner.class);

    public interface ReadQuerySpannerOptions extends DataflowPipelineOptions {
        @Description("Instance ID")
        @Default.String("test-instance")
        String getInstance();

        void setInstance(String value);

        @Description("Query to run")
        @Default.String("SELECT * FROM Singers")
        String getQuery();

        void setQuery(String value);

        @Description("Database ID")
        @Default.String("example-db")
        String getDatabase();

        void setDatabase(String value);

        @Nullable
        @Description("Project ID")
        String getProjectSpanner();

        void setProjectSpanner(String value);
    }

    public static void main(String[] args) {
        ReadQuerySpannerOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadQuerySpannerOptions.class);

        Pipeline p = Pipeline.create(options);

        // Use Dataflow project if ProjectSpanner not passed
        String project = (options.getProjectSpanner() == null) ? options.getProject() : options.getProjectSpanner();

        p
                .apply(SpannerIO.read()
                        .withInstanceId(options.getInstance())
                        .withDatabaseId(options.getDatabase())
                        .withQuery(options.getQuery())
                        .withProjectId(project)
                )
                .apply("Process Row", ParDo.of(new DoFn<Struct, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                Struct struct = c.element();
                                Long singerId = struct.getLong("SingerId");
                                String firstName = struct.getString("FirstName");
                                String lastName = struct.getString("LastName");

                                String row = String.format("ID %d, First name %s, Last name %s", singerId, firstName, lastName);
                                LOG.info(row);
                                c.output(row);
                            }
                        })
                );

        p.run();
    }
}
