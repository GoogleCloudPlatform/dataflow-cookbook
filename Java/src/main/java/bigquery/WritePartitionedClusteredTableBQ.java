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
import com.google.api.services.bigquery.model.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritePartitionedClusteredTableBQ {
    private static final Logger LOG = LoggerFactory.getLogger(WriteBQ.class);

    public interface WritePartitionedClusteredTableBQOptions extends PipelineOptions {
        @Description("Table to write to")
        String getTable();

        void setTable(String value);

        @Description("Dataset to write to")
        String getDataset();

        void setDataset(String value);
    }

    public static void main(String[] args) {

        WritePartitionedClusteredTableBQOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WritePartitionedClusteredTableBQOptions.class);

        Pipeline p = Pipeline.create(options);

        final List<String> elements = Arrays.asList(
                "John, 2021-02-01, USA",
                "Charles, 2021-02-04, USA",
                "Alice, 2021-02-02, Spain",
                "Bob, 2021-02-07, USA",
                "Amanda, 2021-02-06, France",
                "Alex, 2021-02-02, Mexico",
                "Eliza, 2021-02-04, Japan"
        );

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("date").setType("DATE"));
        fields.add(new TableFieldSchema().setName("country").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        // Partitioning
        TimePartitioning partitioning = new TimePartitioning();
        partitioning.setType("DAY").setField("date");

        // Clustering
        Clustering clustering = new Clustering();
        clustering.setFields(Collections.singletonList("country"));

        p
                .apply(Create.of(elements))
                //Convert to TableRow
                .apply("to TableRow", ParDo.of(new DoFn<String, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] columns = c.element().split(", ");

                        TableRow row = new TableRow();

                        row.set("name", columns[0]);
                        row.set("date", columns[1]);
                        row.set("country", columns[2]);

                        c.output(row);
                    }
                }))
                // to BigQuery
                // Using `writeTableRows` is slightly less performant than using write with `WithFormatFunction`
                // due to the TableRow encoding. See `WriteWithFormatBQ` for an example.
                .apply(BigQueryIO.writeTableRows() // Input type from prev stage is TableRow
                        .withSchema(schema)
                        .to(options.getTable())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        // add partitioning and clustering
                        .withClustering(clustering)
                        .withTimePartitioning(partitioning));
        p.run();
    }
}
