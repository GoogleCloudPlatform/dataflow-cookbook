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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class RepeatedRowsBQ {

    private static final Logger LOG = LoggerFactory.getLogger(RepeatedRowsBQ.class);

    public interface RepeatedRowsBQOptions extends PipelineOptions {
        @Description("Table to write")
        String getTable();

        void setTable(String value);
    }


    public static void main(String[] args) {
        RepeatedRowsBQOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(RepeatedRowsBQOptions.class);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("ngram").setType("STRING"));
        // For repeated fields we need to setMode
        fields.add(new TableFieldSchema().setName("year").setType("INTEGER").setMode("REPEATED"));

        TableSchema schema = new TableSchema().setFields(fields);

        Pipeline p = Pipeline.create(options);

        String sql = "SELECT term, years " +
                "FROM `bigquery-public-data.google_books_ngrams_2020.spa_1` " +
                "LIMIT 1000";


        p
                .apply(BigQueryIO.readTableRows().fromQuery(sql).usingStandardSql())
                .apply("Repeated Row To New Repeated Row", ParDo.of(new DoFn<TableRow, TableRow>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws ParseException {
                                TableRow originalRow = c.element();

                                String ngram = originalRow.get("term").toString();

                                // Since the field is repeated, we retrieve a List of the type (TableRow since nested)
                                List<TableRow> originalRepeatedField = (List<TableRow>) originalRow.get("years");

                                // Create our repeated object of type INTEGER
                                List<Integer> repeatedField = new ArrayList<Integer>();

                                for (TableRow row : originalRepeatedField) {
                                    Integer year = Integer.parseInt(row.get("year").toString());
                                    if (year > 1950) {
                                        repeatedField.add(year);
                                    }
                                }

                                TableRow outputRow = new TableRow()
                                        .set("ngram", ngram)
                                        .set("year", repeatedField);

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
