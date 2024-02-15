/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jdbc;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class WriteJdbc {

    /**
     * Represents a row in the Example table.
     */
    public static class ExampleRow implements Serializable {

        private int id;
        private String name;

        public ExampleRow() {}

        public ExampleRow(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * Pipeline options for write to JDBC.
     */
    public interface WriteJdbcOptions extends PipelineOptions {
        @Description("Table name to write to")
        @Validation.Required
        String getTableName();

        void setTableName(String tableName);

        @Description("JDBC URL")
        @Validation.Required
        String getJdbcUrl();

        void setJdbcUrl(String jdbcUrl);

        @Description("")
        @Default.String("org.postgresql.Driver")
        String getDriverClassName();

        void setDriverClassName(String driverClassName);

        @Description("DB Username")
        @Validation.Required
        String getUsername();

        void setUsername(String username);

        @Description("DB password")
        @Validation.Required
        String getPassword();

        void setPassword(String password);
    }

    public static void main(String[] args) {
        WriteJdbcOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteJdbcOptions.class);

        // Configuration for JDBC connection
        JdbcIO.DataSourceConfiguration config =
                JdbcIO.DataSourceConfiguration.create(options.getDriverClassName(), options.getJdbcUrl())
                        .withUsername(options.getUsername())
                        .withPassword(options.getPassword());

        Pipeline p = Pipeline.create(options);

        List<ExampleRow> rows = Arrays.asList(
                new ExampleRow(1, "Charles"),
                new ExampleRow(2, "Alice"),
                new ExampleRow(3, "Bob"),
                new ExampleRow(4, "Amanda"),
                new ExampleRow(5, "Alex"),
                new ExampleRow(6, "Eliza")
        );

        p.apply("Create", Create.of(rows))
        .apply(
            "Write to JDBC",
            JdbcIO.<ExampleRow>write()
                .withDataSourceConfiguration(config)
                .withStatement(String.format("insert into %s values(?, ?)", options.getTableName()))
                .withBatchSize(10L)
                .withPreparedStatementSetter(
                    (element, statement) -> {
                      statement.setInt(1, element.getId());
                      statement.setString(2, element.getName());
                    }));

        p.run();
    }
}
