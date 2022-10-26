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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

public class ReadCloudSQL {

    private static final Logger LOG = LoggerFactory.getLogger(ReadCloudSQL.class);

    public interface ReadCloudSQLOptions extends PipelineOptions {
        @Description("query")
        @Default.String("SELECT * FROM entries ")
        String getQuery();

        void setQuery(String value);
    }

    public static void main(String[] args) {
        // Reference the extended class
        ReadCloudSQLOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadCloudSQLOptions.class);

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

        JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create("com.mysql.cj.jdbc.Driver", jdbcURL);

        p
                .apply(JdbcIO.<String>read()
                        .withDataSourceConfiguration(config)
                        .withQuery(options.getQuery())
                        .withCoder(StringUtf8Coder.of())
                        .withRowMapper(new JdbcIO.RowMapper<String>() {
                            public String mapRow(ResultSet resultSet) throws Exception {
                                String guestName = resultSet.getString("guestName");
                                String content = resultSet.getString("content");
                                return "Guest is " + guestName + " and they said: " + content;
                            }
                        }))
                .apply("Log", ParDo.of(new DoFn<String, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                LOG.info(c.element());
                                c.output(c.element());
                            }
                        })
                );

        p.run();
    }


}
