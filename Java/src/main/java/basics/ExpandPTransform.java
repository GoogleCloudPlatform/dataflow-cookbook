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
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ExpandPTransform {

    private static final Logger LOG = LoggerFactory.getLogger(ExpandPTransform.class);

    public static class WordCount extends PTransform<PCollection<String>, PCollection<KV<String, Integer>>> {
        @Override
        public PCollection<KV<String, Integer>> expand(PCollection<String> words) {
            // Lines to words
            PCollection<KV<String, Integer>> wordCount = words
                    .apply(FlatMapElements.into(TypeDescriptors.strings())
                            .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
                    .apply(Filter.by((String word) -> !word.isEmpty()))
                    .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                            .via((String word) -> KV.of(word, 1)))
                    .apply(Sum.integersPerKey()); // aggregate values

            return wordCount;
        }
    }

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        final List<String> elements = Arrays.asList(
                "From fairest creatures we desire increase",
                "That thereby beauty’s rose might never die",
                "But as the riper should by time decease",
                "His tender heir might bear his memory",
                "But thou, contracted to thine own bright eyes",
                "Feed’st thy light’st flame with self-substantial fuel",
                "Making a famine where abundance lies",
                "Thyself thy foe, to thy sweet self too cruel",
                "Thou that art now the world’s fresh ornament",
                "And only herald to the gaudy spring",
                "Within thine own bud buriest thy content",
                "And, tender churl, makest waste in niggarding",
                "Pity the world, or else this glutton be",
                "To eat the world’s due, by the grave and thee"
        );

        Pipeline p = Pipeline.create(options);

        p
                .apply("Create Elements", Create.of(elements))
                .apply(new WordCount())
                .apply(ParDo.of(new DoFn<KV<String, Integer>, String>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, Integer> element, OutputReceiver<String> out) {
                        String count = element.getKey() + ": " + element.getValue();
                        LOG.info(count);
                        out.output(count);
                    }
                }));


        p.run();
    }


}
