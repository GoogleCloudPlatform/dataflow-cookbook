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

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JVMInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(JVMInitializer.class);

    @AutoService(JvmInitializer.class)
    public static class TestInitializer implements JvmInitializer {
        @Override
        public void onStartup() {
            LOG.info("Setting up Java VM Initializer");
            System.setProperty("javax.net.debug", "ssl:verbose");
        }

        @Override
        public void beforeProcessing(PipelineOptions options) {
        }
    }

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        p
                .apply("Create Elements", Create.of(1, 2, 3, 4, 5))
                .apply("Log", ParDo.of(new DoFn<Integer, Integer>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                c.output(c.element());
                            }
                        })
                );

        p.run();
    }
}
