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
import org.apache.beam.sdk.options.PipelineOptions;
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

public class BasicSQL {

    private static final Logger LOG = LoggerFactory.getLogger(BasicSQL.class);

    public static final Schema rowSchema = Schema.builder()
            .addStringField("name")
            .addInt32Field("year")
            .addStringField("country")
            .build();

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

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

        p
                .apply(Create.of(elements))
                //Convert to Row
                .apply("to Row", ParDo.of(new DoFn<String, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] columns = c.element().split(", ");

                        Row row = Row
                                .withSchema(rowSchema)
                                .addValues(columns[0], Integer.parseInt(columns[1]), columns[2])
                                .build();
                        c.output(row);
                    }
                })).setRowSchema(rowSchema) // Output needs an schema
                // SQL
                .apply(SqlTransform.query(
                        "SELECT COUNT(*) AS `count`, `year`, country FROM PCOLLECTION GROUP BY `year`, country")) // year and count with `` since are part of Calcite SQL
                .apply("Log", ParDo.of(new DoFn<Row, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                Row row = c.element();
                                Long count = row.getInt64("count"); // COUNT is type Long
                                Integer year = row.getInt32(1); // You can use the index
                                String country = row.getString("country");
                                String finalString = String.format("Country %s has total %d for year %d", country, count, year);
                                LOG.info(finalString);
                                c.output(finalString);
                            }
                        })
                );
        p.run();
    }
}
