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
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WriteDynamicBQ {

    private static final Logger LOG = LoggerFactory.getLogger(WriteDynamicBQ.class);

    public interface WriteDynamicBQOptions extends DataflowPipelineOptions {
        @Description("Dataset to write to")
        String getDataset();

        void setDataset(String value);
    }

    public static void main(String[] args) {

        WriteDynamicBQOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteDynamicBQOptions.class);

        Pipeline p = Pipeline.create(options);

        final List<String> elements = Arrays.asList(
                "John, 1990, USA, group1",
                "Charles, 1995, USA, group2",
                "Alice, 1997, Spain, group2",
                "Bob, 1995, USA, group1",
                "Amanda, 1991, France, group2",
                "Alex, 1999, Mexico, group1",
                "Eliza, 2000, Japan, group2"
        );

        String dataset = options.getDataset();


        p
                .apply(Create.of(elements))
                //Convert to TableRow
                .apply("to TableRow", ParDo.of(new DoFn<String, KV<String, TableRow>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] columns = c.element().split(", ");

                        TableRow row = new TableRow();
                        row.set("name", columns[0]);
                        row.set("year", columns[1]);
                        row.set("country", columns[2]);

                        String group = columns[3];
                        if (group.equals("group2")) {
                            row.set("group", columns[3]);
                        }
                        c.output(KV.of(group, row));
                    }
                }))
                // to BigQuery
                .apply(BigQueryIO.<KV<String, TableRow>>write()
                        .withFormatFunction(KV::getValue)
                        .to(new DynamicDestinations<KV<String, TableRow>, String>() {
                            public String getDestination(ValueInSingleWindow<KV<String, TableRow>> element) {
                                return element.getValue().getKey();
                            }

                            public TableDestination getTable(String group) {
                                String table = dataset + "." + group;
                                return new TableDestination(table, "Table for group " + group);
                            }

                            public TableSchema getSchema(String group) {

                                List<TableFieldSchema> fields = new ArrayList<>();
                                fields.add(new TableFieldSchema().setName("name").setType("STRING"));
                                fields.add(new TableFieldSchema().setName("year").setType("INTEGER"));
                                fields.add(new TableFieldSchema().setName("country").setType("STRING"));

                                if (group.equals("group2")) {
                                    fields.add(new TableFieldSchema().setName("group").setType("STRING"));
                                }

                                TableSchema schema = new TableSchema().setFields(fields);
                                return schema;
                            }
                        })
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        p.run();
    }
}
