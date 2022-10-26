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
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class coGroupByKey {
    private static final Logger LOG = LoggerFactory.getLogger(coGroupByKey.class);

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        final List<KV<String, String>> jobs = Arrays.asList(
                KV.of("Anna", "SWE"),
                KV.of("Kim", "Data Engineer"),
                KV.of("Kim", "Data Scientist"),
                KV.of("Robert", "Artist"),
                KV.of("Sophia", "CEO")
        );

        final List<KV<String, String>> hobbies = Arrays.asList(
                KV.of("Anna", "Painting"),
                KV.of("Kim", "Football"),
                KV.of("Kim", "Gardening"),
                KV.of("Robert", "Swimming"),
                KV.of("Sophia", "Mathematics"),
                KV.of("Sophia", "Tennis")
        );

        final TupleTag<String> jobsTag = new TupleTag<>();
        final TupleTag<String> hobbiesTag = new TupleTag<>();

        PCollection<KV<String, String>> jobsPcol = p.apply("Create Jobs", Create.of(jobs));
        PCollection<KV<String, String>> hobbiesPcol = p.apply("Create Hobbies", Create.of(hobbies));

        KeyedPCollectionTuple.of(jobsTag, jobsPcol).and(hobbiesTag, hobbiesPcol)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Iterable<String> jobs = c.element().getValue().getAll(jobsTag); // gets elements for Jobs
                        Iterable<String> hobbies = c.element().getValue().getAll(hobbiesTag);

                        // Cross join
                        for (String job : jobs) {
                            for (String hobby : hobbies) {
                                String description = "has job " + job + " and hobby " + hobby + ".";
                                KV<String, String> kv = KV.of(c.element().getKey(), description);
                                LOG.info(kv.toString());
                                c.output(kv);
                            }
                        }
                    }
                }));

        p.run();
    }
}
