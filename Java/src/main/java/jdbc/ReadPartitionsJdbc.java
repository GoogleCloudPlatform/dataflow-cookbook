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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.Objects;

public class ReadPartitionsJdbc {

  private static final Logger LOG = LoggerFactory.getLogger(ReadPartitionsJdbc.class);

  /**
   * Represents a row in the Example table.
   */
  public static class ExampleRow implements Serializable {
    public static final String ID_COLUMN = "id";
    public static final String NAME_COLUMN = "name";

    private int id;
    private String name;

    public ExampleRow() {}

    public ExampleRow(int id, String name) {
      this.id = id;
      this.name = name;
    }

    @Override
    public String toString() {
      return "ExampleRow{" + "id=" + id + ", name='" + name + '\'' + '}';
    }
  }

  /**
   * Custom RowMapper for mapping rows from the JDBC result set to ExampleRow objects.
   */
  public static class CreateExampleRowOfNameAndId implements JdbcIO.RowMapper<ExampleRow> {
    @Override
    public ExampleRow mapRow(ResultSet resultSet) throws Exception {
      return new ExampleRow(
          Long.valueOf(resultSet.getLong(ExampleRow.ID_COLUMN)).intValue(),
          resultSet.getString(ExampleRow.NAME_COLUMN));
    }
  }

  /**
   * Pipeline options for reading from JDBC.
   */
  public interface ReadJdbcOptions extends PipelineOptions {
    @Description("Table name to read from")
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
    ReadJdbcOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadJdbcOptions.class);

    // Configuration for JDBC connection
    JdbcIO.DataSourceConfiguration config =
        JdbcIO.DataSourceConfiguration.create(options.getDriverClassName(), options.getJdbcUrl())
            .withUsername(options.getUsername())
            .withPassword(options.getPassword());

    Pipeline p = Pipeline.create(options);

    p.apply(
            "Read from jdbc",
            JdbcIO.<ExampleRow>readWithPartitions()
                .withDataSourceConfiguration(config)
                .withRowMapper(new CreateExampleRowOfNameAndId())
                .withTable(options.getTableName())
                .withNumPartitions(1)
                .withPartitionColumn(ExampleRow.ID_COLUMN)
                .withLowerBound(0L)
                .withUpperBound(1000L))
        .apply(
            "Log Data",
            ParDo.of(
                new DoFn<ExampleRow, ExampleRow>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    LOG.info(Objects.requireNonNull(c.element()).toString());
                    c.output(c.element());
                  }
                }));

    p.run();
  }
}
