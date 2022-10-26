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

package basics;

import com.google.common.primitives.Ints;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.IntStream;

public class ParDoWithOutputs {
    private static final Logger LOG = LoggerFactory.getLogger(ParDoWithOutputs.class);

    public static class LogDoFn extends DoFn<Integer, Integer> {
        String extraLog;

        // This allow us to pass a variable to ParDo
        LogDoFn(String extraLog) {
            this.extraLog = extraLog;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info("Element " + c.element().toString() + " is in " + extraLog);
            c.output(c.element());
        }
    }

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        int[] rangeIntegers = IntStream.range(0, 100).toArray();
        Iterable<Integer> elements = Ints.asList(rangeIntegers);

        // Define the outputs with type of the output
        final TupleTag<Integer> multiple3 = new TupleTag<Integer>() {
        };
        final TupleTag<Integer> multiple5 = new TupleTag<Integer>() {
        };
        final TupleTag<Integer> both = new TupleTag<Integer>() {
        };
        final TupleTag<Integer> all = new TupleTag<Integer>() {
        };

        // PCollectionTuple since multiple outputs
        PCollectionTuple create = p
                .apply("Create Elements", Create.of(elements))
                .apply("Split Multiple 3, 5, both or none", ParDo.of(new DoFn<Integer, Integer>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                Integer n = c.element();
                                if (n % 15 == 0) {
                                    c.output(both, n); // both output
                                }
                                if (n % 3 == 0) {
                                    c.output(multiple3, n); // multiple of 3 output
                                }
                                if (n % 5 == 0) {
                                    c.output(multiple5, n); // multiple of 5 output
                                }
                                // we output all no matter what to "all" output
                                c.output(n); // no need to tag it since it's the general

                            }
                        }).withOutputTags(all, TupleTagList.of(Arrays.asList(multiple3, multiple5, both))) // general + other outputs
                );

        create.get(multiple3) // get output for multiples 3
                .apply("Multiple 3", ParDo.of(new LogDoFn("Multiple of 3")));

        create.get(multiple5) // get output for multiples 5
                .apply("Multiple 5", ParDo.of(new LogDoFn("Multiple of 5")));

        create.get(both) // get output for multiples of both
                .apply("Multiple both", ParDo.of(new LogDoFn("Multiple of 3 and 5")));

        create.get(all) // get output for multiples of all
                .apply("All", ParDo.of(new LogDoFn("All")));

        p.run();
    }
}
