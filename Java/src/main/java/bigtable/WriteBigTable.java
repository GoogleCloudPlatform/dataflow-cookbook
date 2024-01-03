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

package bigtable;

import com.google.bigtable.v2.Mutation;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

import java.math.BigInteger;
import java.util.stream.IntStream;

public class WriteBigTable {
    /*
    TODO

    This pipeline needs a BigTable instance and tables, in this example we use
    quickstart examples:
    https://cloud.google.com/bigtable/docs/quickstart-cbt

    Two families need to be created, "prime" and "not prime"
    $ cbt createfamily my-table prime
    $ cbt createfamily my-table not-prime
     */

    /**
     * Pipeline options to be passed via the command line.
     */
    public interface WriteBigTableOptions extends DataflowPipelineOptions {
        @Description("Instance ID")
        @Default.String("quickstart-instance")
        String getInstance();

        void setInstance(String value);

        @Description("Table ID")
        @Default.String("my-table")
        String getTable();

        void setTable(String value);

        @Nullable
        @Description("Project ID")
        String getProjectBT();

        void setProjectBT(String value);
    }

    public static void main(String[] args) {

        // Parse pipeline options from the command line.
        WriteBigTableOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteBigTableOptions.class);

        // Use Dataflow project if ProjectBT pipeline option is not passed.
        String project = (options.getProjectBT() == null) ? options.getProject() : options.getProjectBT();

        // Create the pipeline.
        Pipeline p = Pipeline.create(options);

        int[] rangeIntegers = IntStream.range(0, 100).toArray();
        Iterable<Integer> elements = Ints.asList(rangeIntegers);

        p
                // Add a source that uses the hard-coded values (integers from 0 to 99).
                .apply(Create.of(elements))
                // Add the CreateMutations transform that groups the values into bundles
                // of Bigtable "mutations".
                .apply("Group in Mutations", ParDo.of(new CreateMutations()))
                // The Bigtable sink takes as input a PCollection<KV<ByteString, Iterable<Mutation>>>,
                // where the ByteString is the key of the row being mutated, and each Mutation
                // represents an idempotent transformation to that row.
                .apply(BigtableIO.write()
                        .withInstanceId(options.getInstance())
                        .withProjectId(project)
                        .withTableId(options.getTable()));

        // Execute the pipeline.
        p.run();
    }

    public static class CreateMutations extends DoFn<Integer, KV<ByteString, Iterable<Mutation>>> {
        public ImmutableList.Builder<Mutation> mutations;

        @ProcessElement
        public void processElement(ProcessContext c) {
            BigInteger b = new BigInteger(c.element().toString());
            String isPrime = b.isProbablePrime(1) ? "prime" : "not-prime";

            Mutation.SetCell setCell =
                Mutation.SetCell.newBuilder()
                    .setFamilyName(isPrime)
                    .setColumnQualifier(toByteString(c.element().toString()))
                    .setValue(toByteString("value-" + c.element()))
                    .setTimestampMicros(Instant.now().getMillis() * 1000)
                    .build();
            this.mutations.add(Mutation.newBuilder().setSetCell(setCell).build());
        }

        @StartBundle
        public void startBundle() {
            this.mutations = ImmutableList.builder();
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext c) {
            KV<ByteString, Iterable<Mutation>> row = KV.of(toByteString("numbers"), this.mutations.build());
            c.output(row, Instant.now(), GlobalWindow.INSTANCE);
        }

        private static ByteString toByteString(String value) {
            return ByteString.copyFromUtf8(value);
        }
    }
}
