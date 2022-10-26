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

package gcs;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatchAllFileIO {

    private static final Logger LOG = LoggerFactory.getLogger(MatchAllFileIO.class);

    public static void main(String[] args) {
        // Reference the extended class
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        p
                .apply(Create.of(
                        "gs://apache-beam-samples/shakespeare/kinglear.txt",
                        "gs://apache-beam-samples/shakespeare/macbeth.txt",
                        "gs://apache-beam-samples/shakespeare/a*")) // Wildcards are also valid
                .apply(FileIO.matchAll()) // gets the files
                .apply(FileIO.readMatches()) // converts matches into readable files
                .apply(TextIO.readFiles()) // actual read
                .apply(Count.globally())
                .apply(ParDo.of(new DoFn<Long, Long>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("Total lines " + c.element().toString());
                        c.output(c.element());
                    }
                }))
        ;

        p.run();
    }
}
