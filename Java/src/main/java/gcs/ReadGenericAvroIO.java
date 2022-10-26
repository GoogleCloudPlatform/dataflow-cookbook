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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadGenericAvroIO {

    private static final Logger LOG = LoggerFactory.getLogger(ReadGenericAvroIO.class);

    public interface ReadGenericAvroIOOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://cloud-samples-data/bigquery/us-states/us-states.avro")
        String getInput();

        void setInput(String value);
    }

    public static void main(String[] args) {
        ReadGenericAvroIOOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadGenericAvroIOOptions.class);

        Pipeline p = Pipeline.create(options);

        String fieldsString = "[{ \"name\": \"name\", \"type\": \"string\" }, { \"name\": \"post_abbr\", \"type\": \"string\" }]";
        String schemaString = "{\"type\": \"record\", \"name\": \"WriteAvroIO\",\"fields\":" + fieldsString + "}";
        Schema avroSchema = Schema.parse(schemaString);

        p
                .apply(AvroIO.readGenericRecords(avroSchema).from(options.getInput()))
                .apply("Parse Generic and log", ParDo.of(new DoFn<GenericRecord, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        GenericRecord record = c.element();
                        String stateString = "State " + record.get("name") + " has abbreviation " + record.get("post_abbr");
                        LOG.info(stateString);
                        c.output(stateString);
                    }
                }));

        p.run();
    }
}
