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

package basics;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SideInputs {
    private static final Logger LOG = LoggerFactory.getLogger(SideInputs.class);

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        final List<KV<String, Double>> elements = Arrays.asList(
                KV.of("USD", 3.1415),
                KV.of("USD", 1729.0),
                KV.of("CHF", 2.7182),
                KV.of("EUR", 1.618),
                KV.of("CHF", 1.1)
        );

        final List<KV<String, Double>> rateToUSD = Arrays.asList(
                KV.of("USD", 1.0),
                KV.of("EUR", 0.83),
                KV.of("CHF", 0.9)
        );

        // Side input
        final PCollectionView<Map<String, Double>> ratesView = p
                .apply(Create.of(rateToUSD))
                .apply(View.asMap());

        // Main pipeline
        p
                .apply("Create Elements", Create.of(elements))
                .apply("Convert to USD", ParDo.of(new DoFn<KV<String, Double>, KV<String, Double>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                Map<String, Double> rates = c.sideInput(ratesView);

                                String originalCurrency = c.element().getKey();
                                Double originalValue = c.element().getValue();

                                Double convertedValue = originalValue * rates.get(originalCurrency);
                                LOG.info(originalValue + " " + originalCurrency + " are a total of USD " + convertedValue);
                                c.output(KV.of("USD", convertedValue));
                            }
                        }).withSideInputs(ratesView) // point to side input
                );

        p.run();
    }
}
