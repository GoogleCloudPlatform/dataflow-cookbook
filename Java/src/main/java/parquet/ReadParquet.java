/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet;

import java.io.Serializable;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadParquet {

    private static final Logger LOG = LoggerFactory.getLogger(ReadParquet.class);

    /** Represents an Example Parquet record. */
    public static class ExampleRecord implements Serializable {
        public static final String ID_COLUMN = "id";
        public static final String NAME_COLUMN = "name";

        private int id;
        private String name;
    }

    /** Pipeline options for read from Parquet files. */
    public interface ReadParquetOptions extends PipelineOptions {
        @Description("A file glob pattern to read Parquet from")
        @Validation.Required
        String getFilePattern();

        void setFilePattern(String filePattern);
    }

    public static void main(String[] args) {
        ReadParquetOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadParquetOptions.class);

        Schema exampleRecordSchema = ReflectData.get().getSchema(ExampleRecord.class);

        Pipeline p = Pipeline.create(options);

        p.apply(
                "Read from Parquet",
                ParquetIO.read(exampleRecordSchema)
                    .withAvroDataModel(GenericData.get())
                    .from(options.getFilePattern()))
            .apply(
                "Log Data",
                ParDo.of(
                    new DoFn<GenericRecord, GenericRecord>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            GenericRecord record = Objects.requireNonNull(c.element());
                            LOG.info(
                                "Id = {}, Name = {}",
                                record.get(ExampleRecord.ID_COLUMN),
                                record.get(ExampleRecord.NAME_COLUMN));
                            c.output(record);
                        }
                    }));
        p.run();
    }
}
