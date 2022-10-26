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
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class ParDoInterface {

    // ParDo
    public static class GroupElements extends DoFn<Integer, Integer> {
        private static final Logger LOG = LoggerFactory.getLogger(GroupElements.class);

        List<Integer> bundleElements;

        // Called once per DoFn instance
        @Setup
        public void setup() {
            LOG.info("Setup");
        }

        // Called once per DoFn bundle
        @StartBundle
        public void startBundle() {
            LOG.info("Starting Bundle");
            // reset bundle bag and timestamp
            this.bundleElements = new ArrayList<Integer>();
        }

        // Called for every element
        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            this.bundleElements.add(c.element());
        }

        // Called when bundle finishes
        @FinishBundle
        public void finishBundle(FinishBundleContext c) {
            LOG.info("Size of bundle: " + this.bundleElements.size());

            for (Integer element : this.bundleElements) {
                // output elements in FinishBundle need to have a timestamp and window (since a bundle may contain
                // different windows and timestamps). Instant Zero and GlobalWindow are the default in Batch
                c.output(element, Instant.ofEpochSecond(0), GlobalWindow.INSTANCE);
            }
        }
    }

    public static void main(String[] args) {

        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(DataflowPipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        int[] rangeIntegers = IntStream.range(0, 1000).toArray();
        Iterable<Integer> elements = Ints.asList(rangeIntegers);

        p
                .apply(Create.of(elements))
                .apply(Reshuffle.<Integer>viaRandomKey().withNumBuckets(100))  // Reshuffling so more bundles are created
                .apply(ParDo.of(new GroupElements()));

        p.run();
    }
}
