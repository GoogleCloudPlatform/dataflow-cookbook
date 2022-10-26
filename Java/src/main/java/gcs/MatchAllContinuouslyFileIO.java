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
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.sdk.transforms.Watch.Growth.afterTimeSinceNewOutput;

public class MatchAllContinuouslyFileIO {
    // NOTE: This pipelines requires preparation. You would need to add elements to your bucket

    private static final Logger LOG = LoggerFactory.getLogger(MatchAllContinuouslyFileIO.class);

    public interface MatchAllContinuouslyFileIOOptions extends PipelineOptions {
        @Description("Path prefix to continously check for new files")
        String getPath();

        void setPath(String value);
    }

    public static void main(String[] args) {
        // Reference the extended class
        MatchAllContinuouslyFileIOOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MatchAllContinuouslyFileIOOptions.class);

        Pipeline p = Pipeline.create(options);

        Integer windowLength = 5;

        p
                .apply(FileIO.match()
                        .filepattern(options.getPath() + "*")  // path to look for new files, adding * as wildcard
                        .continuously(Duration.standardSeconds(60),  // checks for files every 60 seconds
                                afterTimeSinceNewOutput(Duration.standardHours(1))  // stops if no new files in 1 hour
                        )
                )
                .apply(FileIO.readMatches()) // converts matches into readable files
                .apply(TextIO.readFiles()) // actual read
                .apply(Window.<String>into(
                        FixedWindows.of(Duration.standardMinutes(windowLength)))
                        .withAllowedLateness(Duration.standardMinutes(windowLength))
                        .discardingFiredPanes()) // Since streaming, we need a window
                .apply(Combine.globally(Count.<String>combineFn()).withoutDefaults())  // Count lines in the window
                .apply("Log Count", ParDo.of(new DoFn<Long, Long>() {
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
