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
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class groupByKey {
    private static final Logger LOG = LoggerFactory.getLogger(groupByKey.class);

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        final List<KV<String, String>> elements = Arrays.asList(
                KV.of("Mammal", "Dog"),
                KV.of("Mammal", "Cat"),
                KV.of("Fish", "Salmon"),
                KV.of("Amphibian", "Snake"),
                KV.of("Bird", "Eagle"),
                KV.of("Bird", "Owl"),
                KV.of("Mammal", "Dolphin")
        );

        p
                .apply("Create Elements", Create.of(elements))
                // Input needs to be KV
                .apply(GroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Iterable<String> values = c.element().getValue();
                        String animals = "Example animals are: ";
                        for (String value : values) {
                            animals = animals + value + ", ";
                        }
                        animals = animals + "etc.";
                        KV<String, String> kv = KV.of(c.element().getKey(), animals);
                        LOG.info(kv.toString());
                        c.output(kv);
                    }
                }));
        p.run();
    }

}
