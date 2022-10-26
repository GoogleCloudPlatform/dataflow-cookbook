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

package bigtable;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadBigTable {
    /*
    TODO

    This pipeline needs a BigTable instance and tables, in this example we use
    quickstart examples:
    https://cloud.google.com/bigtable/docs/quickstart-cbt
    */

    private static final Logger LOG = LoggerFactory.getLogger(ReadBigTable.class);

    public interface ReadBigTableOptions extends DataflowPipelineOptions {
        @Description("Instance ID")
        @Default.String("quickstart-instance")
        String getInstance();

        void setInstance(String value);

        @Description("Table ID")
        @Default.String("my-table")
        String getTable();

        void setTable(String value);

        @Nullable
        @Description("Project ID")
        String getProjectBT();

        void setProjectBT(String value);
    }

    public static void main(String[] args) {
        ReadBigTableOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadBigTableOptions.class);

        // Use Dataflow project if ProjectBT not passed
        String project = (options.getProjectBT() == null) ? options.getProject() : options.getProjectBT();


        Pipeline p = Pipeline.create(options);

        p
                .apply(BigtableIO.read()
                        .withInstanceId(options.getInstance())
                        .withProjectId(project)
                        .withTableId(options.getTable())
                )
                .apply("Process Row", ParDo.of(new DoFn<Row, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                Row row = c.element();
                                DateTimeFormatter format = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");

                                StringBuilder rowString = new StringBuilder("Row: " + row.getKey().toStringUtf8());
                                for (Family family : row.getFamiliesList()) {
                                    StringBuilder familyString = new StringBuilder("\t Family: " + family.getName());
                                    for (Column column : family.getColumnsList()) {
                                        StringBuilder columnString = new StringBuilder("\t\t Column: " + column.getQualifier().toStringUtf8());

                                        for (Cell cell : column.getCellsList()) {
                                            String timestamp = Instant.ofEpochMilli(cell.getTimestampMicros() / 1000).toString(format);
                                            String cellString = "\t\t\t Cell: " + cell.getValue().toStringUtf8()
                                                    + " at " + timestamp;

                                            columnString.append("\n").append(cellString);
                                        }
                                        familyString.append("\n").append(columnString);
                                    }
                                    rowString.append("\n").append(familyString);
                                }

                                LOG.info(rowString.toString());
                                c.output(rowString.toString());
                            }
                        })
                );

        p.run();
    }
}
