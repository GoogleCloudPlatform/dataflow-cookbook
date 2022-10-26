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

package minimal;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class createWithParameters {

    private static final Logger LOG = LoggerFactory.getLogger(create.class);

    // Parameter parser
    public interface MyOptions extends DataflowPipelineOptions {
        @Description("Extra element")
        @Default.Integer(1729)
        Integer getNumber();

        void setNumber(Integer value);
    }

    public static void main(String[] args) {
        // Reference the extended class
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

        Pipeline p = Pipeline.create(options);

        p.
                apply("Create Elements", Create.of(1, 2, 3, 4, 5,
                        options.getNumber())); // Element accessible via options

        p.run();
    }
}
