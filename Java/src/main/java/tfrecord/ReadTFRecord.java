/*
 * Copyright 2024 Google LLC
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
package tfrecord;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadTFRecord {

    private static final Logger LOG =
            LoggerFactory.getLogger(ReadTFRecord.class);

    /**
     * Pipeline options for read from TFRecord.
     */
    public interface ReadTFRecordOptions extends PipelineOptions {

        @Description("A file glob pattern to read TFRecords from")
        @Validation.Required
        String getFilePattern();

        void setFilePattern(String filePattern);
    }

    public static void main(String[] args) {
        ReadTFRecordOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation().as(ReadTFRecordOptions.class);

        Pipeline p = Pipeline.create(options);

        p.apply(
                "Read from TFRecord",
                TFRecordIO.read()
                    .from(options.getFilePattern())
                    .withCompression(Compression.UNCOMPRESSED))
            .apply(
                "Convert to string and log",
                ParDo.of(
                    new DoFn<byte[], String>() {
                        @DoFn.ProcessElement
                        public void processElement(ProcessContext c) {
                            String output =
                                    new String(c.element(), Charsets.UTF_8);
                            LOG.info("Output: {}", output);
                            c.output(output);
                        }
                    }));

        p.run();
    }
}
