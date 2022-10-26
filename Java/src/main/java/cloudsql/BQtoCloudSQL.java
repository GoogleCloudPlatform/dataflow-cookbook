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

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toBeamRow;

public class BQtoCloudSQL {

    private static final Logger LOG = LoggerFactory.getLogger(BQtoCloudSQL.class);

    public interface BQtoCloudSQLOptions extends PipelineOptions {
        @Description("BQ Query")
        @Default.String("SELECT * FROM `bigquery-public-data.census_bureau_international.midyear_population`")
        String getQuery();

        void setQuery(String value);

        @Description("DB table, needs to exist")
        @Default.String("midyear_population")
        String getDBTable();

        void setDBTable(String value);
    }

    public static final Schema rowSchema = Schema.builder()
            .addNullableField("country_code", Schema.FieldType.STRING)
            .addNullableField("country_name", Schema.FieldType.STRING)
            .addNullableField("year", Schema.FieldType.INT32)
            .addNullableField("midyear_population", Schema.FieldType.INT32)
            .build();

    public static void main(String[] args) {
        // Reference the extended class
        BQtoCloudSQLOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BQtoCloudSQLOptions.class);

        Pipeline p = Pipeline.create(options);

        // TODO
        String user = "";
        String password = "";
        String connectionName = "project:region:csqlinstance";
        String database = "bq"; // Needs to exist

        String jdbcURL = "jdbc:mysql://google/" + database + "?cloudSqlInstance=" + connectionName +
                "&socketFactory=com.google.cloud.sql.mysql.SocketFactory&useSSL=false" +
                "&user=" + user + "&password=" + password;

        JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create("com.mysql.cj.jdbc.Driver", jdbcURL);

        p
                .apply(BigQueryIO.readTableRows()
                        .fromQuery(options.getQuery())
                        .usingStandardSql()
                )
                .apply("TableRow to Row", ParDo.of(new DoFn<TableRow, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        TableRow tableRow = c.element();
                        Row row = toBeamRow(rowSchema, tableRow);
                        c.output(row);
                    }
                })).setRowSchema(rowSchema) // Output needs an schema;
                .apply(JdbcIO.<Row>write()
                        .withDataSourceConfiguration(config)
                        .withTable(options.getDBTable())
                        .withBatchSize(2000L) // Default is 1000
                );

        p.run();
    }


}
