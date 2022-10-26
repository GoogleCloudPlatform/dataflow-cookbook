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
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
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

import java.util.regex.Pattern;

public class MatchAllFileIOStreaming {
    // NOTE: This pipelines requires preparation. You would need a topic and publish GCS file paths

    private static final Logger LOG = LoggerFactory.getLogger(MatchAllFileIOStreaming.class);

    public interface matchAllFileIOStreamingOptions extends PipelineOptions {
        @Description("Topic to read from")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {
        // Reference the extended class
        matchAllFileIOStreamingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(matchAllFileIOStreamingOptions.class);

        Pipeline p = Pipeline.create(options);

        Integer windowLength = 5;

        p
                .apply("Read GCS paths", PubsubIO.readStrings().fromTopic(options.getTopic())) // Messages need to be GCS paths
                .apply("Validate GCS", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Pattern p = Pattern.compile("gs://.*/.*");

                        if (p.matcher(c.element()).matches()) {
                            c.output(c.element());
                        } else {
                            LOG.warn("Discarding element " + c.element() + " since it is not a GCS path");
                        }
                    }
                }))
                .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW)) // gets the files
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
                }));

        p.run();
    }
}
