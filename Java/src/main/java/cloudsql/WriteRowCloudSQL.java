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

package cloudsql;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
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

public class WriteRowCloudSQL {

    private static final Logger LOG = LoggerFactory.getLogger(WriteRowCloudSQL.class);

    public interface WriteRowCloudSQLOptions extends PipelineOptions {
        @Description("table")
        @Default.String("persons")
        String getTable();

        void setTable(String value);
    }

    public static final Schema rowSchema = Schema.builder()
            .addStringField("name")
            .addInt32Field("year")
            .addStringField("country")
            .build();

    public static void main(String[] args) {
        // Reference the extended class
        WriteRowCloudSQLOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteRowCloudSQLOptions.class);

        Pipeline p = Pipeline.create(options);

        // TODO
        // https://cloud.google.com/sql/docs/mysql/quickstart
        String user = "";
        String password = "";
        String connectionName = "project:region:csqlinstance";
        String database = "guestbook";

        String jdbcURL = "jdbc:mysql://google/" + database + "?cloudSqlInstance=" + connectionName +
                "&socketFactory=com.google.cloud.sql.mysql.SocketFactory&useSSL=false" +
                "&user=" + user + "&password=" + password;

        JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create("com.mysql.jdbc.Driver", jdbcURL);


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
                .apply(JdbcIO.<Row>write()
                        .withDataSourceConfiguration(config)
                        .withTable(options.getTable())
                );


        p.run();
    }
}
