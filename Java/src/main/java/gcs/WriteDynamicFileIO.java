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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.math.BigInteger;
import java.util.stream.IntStream;

import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;


public class WriteDynamicFileIO {

    public interface writeDynamicFileIOOptions extends PipelineOptions {
        @Description("Output base to write to")
        String getOutput();

        void setOutput(String value);

        @Description("Max Number")
        @Default.Integer(100)
        Integer getMaxNumber();

        void setMaxNumber(Integer value);
    }

    public static void main(String[] args) {
        writeDynamicFileIOOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(writeDynamicFileIOOptions.class);

        Pipeline p = Pipeline.create(options);

        int[] rangeIntegers = IntStream.range(0, options.getMaxNumber()).toArray();
        Iterable<Integer> elements = Ints.asList(rangeIntegers);

        p
                .apply(Create.of(elements))
                .apply(ParDo.of(new DoFn<Integer, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        BigInteger b = new BigInteger(c.element().toString());
                        String key = "not-prime";
                        if (b.isProbablePrime(1)) {
                            key = "prime";
                        }
                        c.output(KV.of(key, c.element().toString()));
                    }
                }))
                // The expected files would have path options.getOutput()/dynamic-file-<prime, not prime>-ID.txt
                // Coming from to .to/.withNaming.by. and the content of the file comes from .via
                .apply("Write File(s)",
                        FileIO.<String, KV<String, String>>writeDynamic()
                                .by(KV::getKey)  // equivalent to <KV>.getKey()
                                .withDestinationCoder(StringUtf8Coder.of())
                                .via(Contextful.fn(KV::getValue), TextIO.sink())  // What we actually write, how we write it -- FileIO expects a Contextful, hence Contextful.fn(KV::getValue)
                                .to(options.getOutput()) // path
                                .withNaming(key -> defaultNaming("dynamic-file-" + key, ".txt"))
                );

        p.run();


    }
}
