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

import com.google.cloud.spanner.Mutation;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WriteGroupedSpanner {
    /*
    TODO

    This pipeline needs a Spanner instance, in this example we use
    quickstart examples:
    https://cloud.google.com/spanner/docs/quickstart-console
    */

    private static final Logger LOG = LoggerFactory.getLogger(WriteGroupedSpanner.class);

    public interface WriteGroupedSpannerOptions extends DataflowPipelineOptions {
        @Description("Instance ID")
        @Default.String("test-instance")
        String getInstance();

        void setInstance(String value);

        @Description("Database ID")
        @Default.String("example-db")
        String getDatabase();

        void setDatabase(String value);

        @Description("Table ID")
        @Default.String("singers")
        String getTable();

        void setTable(String value);

        @Nullable
        @Description("Project ID")
        String getProjectSpanner();

        void setProjectSpanner(String value);
    }

    public static class GroupMutations extends DoFn<String, MutationGroup> {
        /* Spanner needs a primary mutation
        https://cloud.google.com/spanner/docs/dataflow-connector#mutationgroup
        */
        private List<Mutation> mutations;
        private Mutation primary;
        private final String table;

        GroupMutations(String table) {
            this.table = table;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] singer = c.element().split(", ");
            Long singerId = Long.parseLong(singer[0]);

            Mutation mutation = Mutation.newInsertOrUpdateBuilder(this.table)
                    .set("singerId").to(singerId)
                    .set("firstName").to(singer[1])
                    .set("lastName").to(singer[2])
                    .build();
            if (this.primary != null) {
                this.mutations.add(mutation);
            } else {
                this.primary = mutation;
            }
        }

        @StartBundle
        public void startBundle() {
            this.mutations = new ArrayList<Mutation>();
            this.primary = null;
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext c) {

            MutationGroup mutationGroup = MutationGroup.create(this.primary, this.mutations);
            c.output(mutationGroup, Instant.now(), GlobalWindow.INSTANCE);
        }
    }

    public static void main(String[] args) {
        WriteGroupedSpannerOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteGroupedSpannerOptions.class);

        Pipeline p = Pipeline.create(options);

        final List<String> elements = Arrays.asList(
                "40, Anish, Smith",
                "50, Jenifer, Perez",
                "60, John, Johnson",
                "70, Kim, Chan"
        );

        String table = options.getTable();

        // Use Dataflow project if ProjectSpanner not passed
        String project = (options.getProjectSpanner() == null) ? options.getProject() : options.getProjectSpanner();

        p
                .apply(Create.of(elements))
                .apply("To MutationGroup", ParDo.of(new GroupMutations(table)))
                .apply(SpannerIO.write()
                        .withInstanceId(options.getInstance())
                        .withDatabaseId(options.getDatabase())
                        .withProjectId(project)
                        .grouped()
                );

        p.run();
    }
}
