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
package bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Pipeline for writing data to Google Cloud BigQuery using Avro serialization. The pipeline creates
 * an output table in BigQuery and writes {@code ExampleRecord} instances to it. The AvroWriter
 * configuration in the pipeline applies a custom format function with a modified write behavior
 * that involves appending a suffix to string values before writing to BigQuery.
 */
public class WriteWithAvroWriterBQ {

    /** Represents an Example BigQuery record. */
    public static class ExampleRecord implements Serializable {
        public static final String ID_COLUMN = "id";
        public static final String NAME_COLUMN = "name";

        private long id;
        private String name;

        public ExampleRecord() {}

        public ExampleRecord(long id, String name) {
            this.id = id;
            this.name = name;
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    /** Pipeline options for write to BigQuery. */
    public interface WriteBigQueryOptions extends PipelineOptions {
        @Description("Output Google Cloud BigQuery table")
        @Validation.Required
        String getOutputTable();

        void setOutputTable(String outputTable);
    }

    public static void main(String[] args) {
        WriteBigQueryOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteBigQueryOptions.class);

        Pipeline p = Pipeline.create(options);

        List<ExampleRecord> rows =
            Arrays.asList(
                new ExampleRecord(1, "Charles"),
                new ExampleRecord(2, "Alice"),
                new ExampleRecord(3, "Bob"),
                new ExampleRecord(4, "Amanda"),
                new ExampleRecord(5, "Alex"),
                new ExampleRecord(6, "Eliza"));

        TableSchema tableSchema = new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName(ExampleRecord.NAME_COLUMN).setType("STRING"),
                    new TableFieldSchema().setName(ExampleRecord.ID_COLUMN).setType("INTEGER")));

        p.apply("Create",
            Create.of(rows)
                .withCoder(SerializableCoder.of(ExampleRecord.class)))
            .apply(
                BigQueryIO.<ExampleRecord>write()
                    .to(options.getOutputTable())
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withSchema(tableSchema)
                    .withAvroWriter(
                        new ExampleAvroFormatFunction(),
                        s -> new ExampleDatumWriter())
                    .withoutValidation());
        p.run();
    }

    /**
     * Converts instances of {@link ExampleRecord} to {@link GenericRecord} using a provided Avro
     * schema from the Avro write request.
     */
    private static class ExampleAvroFormatFunction
            implements SerializableFunction<AvroWriteRequest<ExampleRecord>, GenericRecord> {

        @Override
        public GenericRecord apply(AvroWriteRequest<ExampleRecord> request) {
            GenericRecordBuilder builder = new GenericRecordBuilder(request.getSchema());
            ExampleRecord record = request.getElement();

            builder
                .set(ExampleRecord.NAME_COLUMN, record.getName())
                .set(ExampleRecord.ID_COLUMN, record.getId());
            return builder.build();
        }
    }

    /**
     * Extends {@link GenericDatumWriter} to modify the writing behavior of strings by appending a
     * suffix to the original string value.
     */
    private static class ExampleDatumWriter extends GenericDatumWriter<GenericRecord> {

        private static final String EXAMPLE_SUFFIX = "_example";

        @Override
        protected void writeString(org.apache.avro.Schema schema, Object datum, Encoder out)
                throws IOException {
            super.writeString(schema, datum.toString() + EXAMPLE_SUFFIX, out);
        }
    }
}
