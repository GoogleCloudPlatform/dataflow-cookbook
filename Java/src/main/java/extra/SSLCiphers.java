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

import com.google.common.base.Joiner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLServerSocketFactory;

public class SSLCiphers {
    /*
    This pipeline lists as a log the Ciphers used by the workers
     */

    private static final Logger LOG = LoggerFactory.getLogger(SSLCiphers.class);

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        p
                .apply("Create Element", Create.of(1))
                .apply("SSL logger", ParDo.of(new DoFn<Integer, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        SSLServerSocketFactory fact = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
                        String ciphers = Joiner.on("\n").join(fact.getSupportedCipherSuites());

                        LOG.info("Manual log of Ciphers: \n" + ciphers);

                        c.output(ciphers);
                    }
                }));

        p.run();
    }
}
