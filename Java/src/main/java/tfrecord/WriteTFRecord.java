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
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Charsets;

import java.util.Arrays;
import java.util.List;

/**
 * Pipeline for writing data to TFRecord files using the {@code TFRecordIO.write()} transform.
 */
public class WriteTFRecord {

    /**
     * Pipeline options for write to TFRecord.
     */
    public interface WriteTFRecordOptions extends PipelineOptions {

        @Description("A file path prefix to write TFRecords files to")
        @Validation.Required
        String getFilePathPrefix();

        void setFilePathPrefix(String filePathPrefix);
    }

    public static void main(String[] args) {
        WriteTFRecordOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation().as(WriteTFRecordOptions.class);

        Pipeline p = Pipeline.create(options);

        List<String> rows = Arrays.asList(
            "Charles", "Alice", "Bob", "Amanda", "Alex", "Eliza"
        );

        p.apply("Create", Create.of(rows))
            .apply(
                "Convert to bytes",
                ParDo.of(
                    new DoFn<String, byte[]>() {
                        @DoFn.ProcessElement
                        public void processElement(ProcessContext c) {
                            c.output(c.element().getBytes(Charsets.UTF_8));
                        }
                    }))
            .apply(
                "Write to TFRecord",
                    TFRecordIO.write()
                        .to(options.getFilePathPrefix())
                        .withCompression(Compression.UNCOMPRESSED)
                        .withNumShards(1));

        p.run();
    }
}
