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
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.stream.IntStream;

public class WriteTextIO {

    public interface writeTextIOOptions extends PipelineOptions {
        @Description("Output to write to")
        String getOutput();

        void setOutput(String value);

        @Description("Max Number")
        @Default.Integer(100)
        Integer getMaxNumber();

        void setMaxNumber(Integer value);
    }

    public static void main(String[] args) {
        // Reference the extended class
        writeTextIOOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(writeTextIOOptions.class);

        Pipeline p = Pipeline.create(options);

        int[] rangeIntegers = IntStream.range(0, options.getMaxNumber()).toArray();
        Iterable<Integer> elements = Ints.asList(rangeIntegers);

        p
                .apply(Create.of(elements))
                .apply(ParDo.of(new DoFn<Integer, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output("This is element number " + c.element().toString());
                    }
                }))
                .apply("Write File(s)",
                        TextIO.write()
                                .to(options.getOutput()));

        p.run();


    }

}
