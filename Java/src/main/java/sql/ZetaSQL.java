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
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ZetaSQL {

    private static final Logger LOG = LoggerFactory.getLogger(ZetaSQL.class);

    public static final Schema rowSchema = Schema.builder()
            .addStringField("name")
            .addInt64Field("year") // ZetaSQL only has Int64 type for integers
            .addStringField("country")
            .build();

    public static class RowToString extends DoFn<Row, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Long total = c.element().getValue("count");
            Long year = c.element().getValue("year");
            String country = c.element().getValue("country");

            String line = "Total: " + total + ", Year: " + year + ", Country: " + country;

            LOG.info(line);
            c.output(line);
        }
    }

    public static void main(String[] args) {

        // This changes to Zeta SQL. Note BeamSqlPipelineOptions rather than PipelineOptions
        BeamSqlPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BeamSqlPipelineOptions.class);
        options.setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner");

        Pipeline p = Pipeline.create(options);

        final List<String> elements = Arrays.asList(
                "John, 1990, USA",
                "Charles, 1995, USA",
                "Alice, 1997, Spain",
                "Bob, 1995, USA",
                "Amanda, 1991, France",
                "Alex, 1999, Mexico",
                "Eliza, 2000, Japan"
        );

        // Create elements
        p.apply(Create.of(elements))
                //Convert to Row
                .apply("to Row", ParDo.of(new DoFn<String, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] columns = c.element().split(", ");

                        Row row = Row
                                .withSchema(rowSchema)
                                .addValues(columns[0], Long.parseLong(columns[1]), columns[2]) // Long because Int64
                                .build();
                        c.output(row);
                    }
                })).setRowSchema(rowSchema) // Output needs an schema
                // SQL
                .apply(SqlTransform.query(
                        "SELECT COUNT(*) AS count, year, country FROM PCOLLECTION GROUP BY year, country"))
                .apply(ParDo.of(new RowToString()));

        p.run();
    }
}
