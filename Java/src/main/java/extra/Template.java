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

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Template {
    /*
    This example shows how to use Classic Templates. Note that Flex Templates
    are the preferred method.
     */

    private static final Logger LOG = LoggerFactory.getLogger(Template.class);

    // Parameter parser
    public interface TemplateOptions extends PipelineOptions {
        @Description("Table to read")
        @Default.String("bigquery-public-data:census_bureau_usa.population_by_zip_2010")
        ValueProvider<String> getTable();

        void setTable(ValueProvider<String> value);

        @Description("Min population")
        @Default.Integer(0)
        ValueProvider<Integer> getMinPopulation();

        void setMinPopulation(ValueProvider<Integer> value);
    }

    static class FilterByPopulation extends DoFn<TableRow, TableRow> {
        ValueProvider<Integer> minPopulation;

        FilterByPopulation(ValueProvider<Integer> minPopulation) {
            this.minPopulation = minPopulation;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();
            Integer population = Integer.parseInt(row.get("population").toString());
            if (population > minPopulation.get()) {
                c.output(c.element());
            }
        }
    }

    public static void main(String[] args) {
        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);

        Pipeline p = Pipeline.create(options);

        p
                .apply(BigQueryIO.readTableRows()
                        .from(options.getTable())
                        // https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.Read.html#withoutValidation--
                        .withoutValidation()
                        .withTemplateCompatibility())
                .apply("Filter", ParDo.of(new FilterByPopulation(options.getMinPopulation())))
                .apply(Count.globally())
                .apply(ParDo.of(new DoFn<Long, Long>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("Total lines " + c.element());
                        c.output(c.element());
                    }
                }));

        p.run();
    }
}
