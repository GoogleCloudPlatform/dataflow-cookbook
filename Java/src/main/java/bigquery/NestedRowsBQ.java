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

package bigquery;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class NestedRowsBQ {

    // Parameter parser
    public interface NestedRowsBQOptions extends PipelineOptions {
        @Description("Table to write")
        String getTable();

        void setTable(String value);
    }


    public static void main(String[] args) {
        // Reference the extended class
        NestedRowsBQOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(NestedRowsBQOptions.class);

        // Nested schema
        List<TableFieldSchema> nestedFields = new ArrayList<>();
        nestedFields.add(new TableFieldSchema().setName("url").setType("STRING"));
        nestedFields.add(new TableFieldSchema().setName("created_at").setType("STRING"));

        // Actual Schema
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("repository").setType("RECORD").setFields(nestedFields));

        TableSchema schema = new TableSchema().setFields(fields);

        Pipeline p = Pipeline.create(options);

        String sql = "SELECT repository, type FROM `bigquery-public-data.samples.github_nested` LIMIT 1000";


        p
                .apply(BigQueryIO.readTableRows().fromQuery(sql).usingStandardSql())
                .apply("Nested Row To New Nested Row", ParDo.of(new DoFn<TableRow, TableRow>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws ParseException {
                                TableRow originalRow = c.element();

                                String type = originalRow.get("type").toString();

                                // Read original nested field
                                TableRow originalNestedField = (TableRow) originalRow.get("repository");

                                String url = originalNestedField.get("url").toString();
                                String createdAt = originalNestedField.get("created_at").toString();

                                // Create a new nested field
                                TableRow nestedField = new TableRow()
                                        .set("url", url)
                                        .set("created_at", createdAt);


                                TableRow outputRow = new TableRow()
                                        .set("type", type)
                                        .set("repository", nestedField);

                                c.output(outputRow);
                            }
                        })
                )
                .apply(BigQueryIO.<TableRow>write()
                        .withSchema(schema)
                        .withFormatFunction((TableRow row) -> row)
                        .to(options.getTable())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run();
    }
}
