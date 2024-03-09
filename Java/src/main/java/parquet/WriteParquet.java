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
package parquet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * Pipeline for writing data to Parquet files using the {@code ParquetIO.sink()} sink.
 */
public class WriteParquet {

    /** Represents an Example Parquet record. */
    public static class ExampleRecord implements Serializable {
        public static final String ID_COLUMN = "id";
        public static final String NAME_COLUMN = "name";

        private int id;
        private String name;

        public ExampleRecord() {}

        public ExampleRecord(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    /** Pipeline options for write to Parquet files. */
    public interface WriteParquetOptions extends PipelineOptions {
        @Description("A file path to write Parquet files to")
        @Validation.Required
        String getFilePath();

        void setFilePath(String filePath);
    }

    public static void main(String[] args) {
        WriteParquetOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteParquetOptions.class);

        Schema exampleRecordSchema = ReflectData.get().getSchema(ExampleRecord.class);

        Pipeline p = Pipeline.create(options);

        List<ExampleRecord> rows =
            Arrays.asList(
                new ExampleRecord(1, "Charles"),
                new ExampleRecord(2, "Alice"),
                new ExampleRecord(3, "Bob"),
                new ExampleRecord(4, "Amanda"),
                new ExampleRecord(5, "Alex"),
                new ExampleRecord(6, "Eliza"));

        p.apply("Create", Create.of(rows))
            .apply(
                "Map to GenericRecord",
                MapElements.via(new MapExampleRecordToGenericRecord(exampleRecordSchema)))
            .setCoder(AvroCoder.of(exampleRecordSchema))
            .apply(
                "Write to Parquet",
                FileIO.<GenericRecord>write()
                    .via(ParquetIO.sink(exampleRecordSchema))
                    .to(options.getFilePath()));
        p.run();
    }

    /**
    * Converts instances of {@link ExampleRecord} to {@link GenericRecord} using a provided Avro
    * schema.
    */
    private static class MapExampleRecordToGenericRecord
      extends SimpleFunction<ExampleRecord, GenericRecord> {

        private final Schema schema;

        public MapExampleRecordToGenericRecord(Schema schema) {
          this.schema = schema;
        }

        @Override
        public GenericRecord apply(ExampleRecord input) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);

            builder
                .set(ExampleRecord.NAME_COLUMN, input.getName())
                .set(ExampleRecord.ID_COLUMN, input.getId());
            return builder.build();
        }
    }
}
