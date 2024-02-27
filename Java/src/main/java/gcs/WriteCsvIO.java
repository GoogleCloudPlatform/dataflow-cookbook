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
package gcs;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.csv.CsvIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.commons.csv.CSVFormat;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Pipeline for writing data to CSV files using the {@code CsvIO.write()} transform.
 */
public class WriteCsvIO {

    /** Represents an Example CSV record. */
    @DefaultSchema(JavaFieldSchema.class)
    public static class ExampleRecord implements Serializable {
        public int id;
        public String name;

        public ExampleRecord() {
        }

        public ExampleRecord(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }
    
    /**
     * Pipeline options for write to CSV files.
     */
    public interface WriteCsvOptions extends PipelineOptions {

        @Description("A file path prefix to write CSV files to")
        @Validation.Required
        String getFilePathPrefix();

        void setFilePathPrefix(String filePathPrefix);
    }

    public static void main(String[] args) {
        WriteCsvOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation().as(WriteCsvOptions.class);

        Pipeline p = Pipeline.create(options);

        List<ExampleRecord> rows =
            Arrays.asList(
                new ExampleRecord(1, "Charles"),
                new ExampleRecord(2, "Alice"),
                new ExampleRecord(3, "Bob"),
                new ExampleRecord(4, "Amanda"),
                new ExampleRecord(5, "Alex"),
                new ExampleRecord(6, "Eliza"));

        CSVFormat csvFormat =
            CSVFormat.DEFAULT.withHeaderComments("example comment 1", "example comment 2")
                .withCommentMarker('#');

        p.apply("Create", Create.of(rows))
            .apply(
                "Write to CSV",
                    CsvIO.<ExampleRecord>write(options.getFilePathPrefix(), csvFormat)
                        .withNumShards(1));
        p.run();
    }
}
