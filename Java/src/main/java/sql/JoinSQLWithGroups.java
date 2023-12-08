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

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinSQLWithGroups {
  private static final Logger LOG = LoggerFactory.getLogger(JoinSQLWithGroups.class);

  public static final Schema nameSchema =
      Schema.builder().addStringField("name").addStringField("family").addInt32Field("age").build();

  public static final Schema jobSchema =
      Schema.builder()
          .addStringField("name")
          .addStringField("job")
          .addDoubleField("salary")
          .build();

  public static Row generateRow(List values, Schema schema) {
    // Create a concrete row with that type.
    Row row = Row.withSchema(schema).addValues(values).build();
    return row;
  }

  public static final List<Row> nameRows =
      Arrays.asList(
          generateRow(Arrays.asList("Carlos", "Alberto", 32), nameSchema),
          generateRow(Arrays.asList("Robert", "Alberto", 25), nameSchema),
          generateRow(Arrays.asList("Alice", "Alberto", 27), nameSchema),
          generateRow(Arrays.asList("Maria", "Serafina", 60), nameSchema),
          generateRow(Arrays.asList("Kim", "Serafina", 42), nameSchema),
          generateRow(Arrays.asList("Bleh", "Serafina", 92), nameSchema));

  public static final List<Row> jobRows =
      Arrays.asList(
          generateRow(Arrays.asList("Carlos", "Backend engineer", 2.0), jobSchema),
          generateRow(Arrays.asList("Robert", "Journalist", 4.0), jobSchema),
          generateRow(Arrays.asList("Alice", "Data Scientist", 8.0), jobSchema),
          generateRow(Arrays.asList("Maria", "Lawyer", 16.0), jobSchema),
          generateRow(Arrays.asList("Kim", "Photographer", 32.0), jobSchema));

  public static void main(String[] args) {

    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

    Pipeline p = Pipeline.create(options);

    // Create elements
    PCollection<Row> namesPcol =
        p.apply("Create Names", Create.of(nameRows)).setRowSchema(nameSchema);
    PCollection<Row> jobsPcol = p.apply("Create Jobs", Create.of(jobRows)).setRowSchema(jobSchema);

    final PCollectionTuple namesAndJobs =
        PCollectionTuple.of("Names", namesPcol).and("Jobs", jobsPcol);

    namesAndJobs
        .apply(
            SqlTransform.query(
                "SELECT Names.family, SUM(Jobs.salary) "
                    + "FROM Names LEFT JOIN Jobs ON Names.name = Jobs.name "
                    + "GROUP BY Names.family"))
        .apply(
            "Log",
            ParDo.of(
                new DoFn<Row, String>() {
                  @ProcessElement
                  public void processElement(@Element Row row, OutputReceiver<String> out) {
                    LOG.info(row.toString());
                    out.output(row.toString());
                  }
                }));

    p.run();
  }
}
