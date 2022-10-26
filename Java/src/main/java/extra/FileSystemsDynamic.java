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

package extra;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class FileSystemsDynamic {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemsDynamic.class);

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        // TODO: change bucket names
        String BUCKET1 = "";
        String BUCKET2 = "";

        final List<KV<String, String>> elements = Arrays.asList(
                KV.of("gs://" + BUCKET1 + "/beam/dynamic.txt", "line"),
                KV.of("gs://" + BUCKET2 + "/beam/dynamic.txt", "line")
        );

        PCollection<KV<String, String>> create = p.
                apply("Create Elements", Create.of(elements));

        create
                .apply("File Systems", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws IOException {
                                // Where to write
                                ResourceId destination = FileSystems.matchNewResource(c.element().getKey(), false);
                                WritableByteChannel writeChannel = FileSystems.create(destination, "text/plain");

                                // What to write
                                ByteBuffer data = ByteBuffer.wrap(c.element().getValue().getBytes(StandardCharsets.UTF_8));

                                // Write
                                writeChannel.write(data);
                                writeChannel.close();
                                c.output(c.element());
                            }
                        })
                );
        p.run();
    }
}
