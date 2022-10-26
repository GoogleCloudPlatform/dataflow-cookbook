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

package sql;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class UDFSQL {

    private static final Logger LOG = LoggerFactory.getLogger(UDFSQL.class);

    public static final Schema rowSchema = Schema.builder()
            .addStringField("name")
            .addStringField("job")
            .addDoubleField("years_worked")
            .build();

    public static Row generateRow(List values, Schema schema) {
        // Create a concrete row with that type.
        Row row = Row
                .withSchema(schema)
                .addValues(values)
                .build();
        return row;
    }

    // Elements
    public final static List<Row> rows = Arrays.asList(
            generateRow(Arrays.asList("Bob", "Journalist", 4.0), rowSchema),
            generateRow(Arrays.asList("Bob", "Bartender", 0.5), rowSchema),
            generateRow(Arrays.asList("Alice", "Data Engineer", 4.0), rowSchema),
            generateRow(Arrays.asList("Alice", "Data Scientist", 1.0), rowSchema),
            generateRow(Arrays.asList("Alice", "Ceo", 2.5), rowSchema)
    );

    // UDF
    public static class MoreThan5Years implements BeamSqlUdf {
        public static Boolean eval(Double input) {
            return input >= 5.0;
        }
    }

    // UDAF
    public static class AppendJobs extends Combine.CombineFn<String, List<String>, String> { //Types: Input, Accum, Output
        @Override
        public List<String> createAccumulator() {
            List<String> list = new ArrayList<>();
            return list;
        }

        @Override
        public List<String> addInput(List<String> accumulator, String input) {
            accumulator.add(input);
            return accumulator;
        }

        @Override
        public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
            List<String> finalList = new ArrayList<>();
            Iterator<List<String>> ite = accumulators.iterator();
            while (ite.hasNext()) {
                finalList.addAll(ite.next());
            }
            return finalList;
        }

        @Override
        public String extractOutput(List<String> accumulators) {
            String result = String.join(", ", accumulators);
            return result;
        }
    }

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        p
                .apply(Create.of(rows)).setRowSchema(rowSchema) // Output needs an schema
                // SQL
                .apply(SqlTransform.query(
                        "SELECT " +
                                "name, " +
                                "SUM(years_worked) total_years_worked, " +
                                "udfYears(SUM(years_worked)) AS more_than_5_years, " +
                                "udafAppend(job) as jobs " +
                                "FROM PCOLLECTION " +
                                "GROUP BY name")
                        .registerUdf("udfYears", MoreThan5Years.class) // UDF
                        .registerUdaf("udafAppend", new AppendJobs()) // UDAF
                )
                .apply("Log", ParDo.of(new DoFn<Row, String>() {
                            @ProcessElement
                            public void processElement(@Element Row row, OutputReceiver<String> out) {
                                LOG.info(row.toString());
                                out.output(row.toString());
                            }
                        })
                );
        p.run();
    }
}
