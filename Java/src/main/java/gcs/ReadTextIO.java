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
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadTextIO {

    private static final Logger LOG = LoggerFactory.getLogger(ReadTextIO.class);


    public interface ReadTextIOOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInput();

        void setInput(String value);
    }

    public static void main(String[] args) {
        ReadTextIOOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadTextIOOptions.class);

        Pipeline p = Pipeline.create(options);

        p
                .apply(TextIO.read().from(options.getInput()))
                .apply(Count.globally())
                .apply(ParDo.of(new DoFn<Long, Long>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("Total lines " + c.element().toString());
                        c.output(c.element());
                    }
                }));

        p.run();


    }
}
