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

import com.google.common.primitives.Ints;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.stream.IntStream;

import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;

public class WriteAvroDynamic {

    private static final Logger LOG = LoggerFactory.getLogger(WriteAvroDynamic.class);

    public interface WriteAvroDynamicOptions extends DataflowPipelineOptions {
        @Description("Output base to write to")
        String getOutput();

        void setOutput(String value);

        @Description("Max Number")
        @Default.Integer(1000)
        Integer getMaxNumber();

        void setMaxNumber(Integer value);
    }

    public static void main(String[] args) {

        WriteAvroDynamicOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteAvroDynamicOptions.class);

        Pipeline p = Pipeline.create(options);

        int[] rangeIntegers = IntStream.range(0, options.getMaxNumber()).toArray();
        Iterable<Integer> elements = Ints.asList(rangeIntegers);

        String schemaString = "{\"type\": \"record\", \"name\": \"avroIOtest\",\"fields\": [{ \"name\": \"number\", \"type\": \"string\" }]}";
        Schema avroSchema = Schema.parse(schemaString);

        p
                .apply(Create.of(elements))
                .apply(ParDo.of(new DoFn<Integer, KV<String, GenericRecord>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Random r = new Random();
                        Integer n = r.nextInt(4);

                        GenericRecord record = new GenericRecordBuilder(avroSchema)
                                .set("number", c.element().toString()).build();

                        c.output(KV.of("key" + n, record));
                    }
                })).setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(GenericRecord.class, avroSchema))) // We need to specify a coder since it depends on the schema.
                .apply("Write File(s)",
                        FileIO.<String, KV<String, GenericRecord>>writeDynamic()
                                .by(KV::getKey)
                                .withDestinationCoder(StringUtf8Coder.of())
                                .via(Contextful.fn(KV::getValue), AvroIO.sink(avroSchema))
                                .to(options.getOutput())
                                .withNaming(key -> defaultNaming("avro-" + key, ".avro"))
                );

        p.run();


    }


}
