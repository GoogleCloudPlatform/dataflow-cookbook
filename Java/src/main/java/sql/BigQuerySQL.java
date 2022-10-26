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

package sql;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
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

public class BigQuerySQL {
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySQL.class);


    public interface BigQuerySQLOptions extends PipelineOptions {
        @Description("Table to write to")
        String getTable();

        void setTable(String value);
    }

    public static final Schema tableSchema = Schema.builder()
            .addStringField("homeTeamName")
            .addInt32Field("year")
            .addDoubleField("avg_attendance")
            .build();

    public static void main(String[] args) {

        BigQuerySQLOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(BigQuerySQLOptions.class);

        Pipeline p = Pipeline.create(options);


        String sql = "SELECT homeTeamName, year, attendance " +
                "FROM `bigquery-public-data.baseball.schedules`";

        p
                .apply(BigQueryIO.readTableRowsWithSchema() // Reading with Schema lets us use the SQL transform directly
                        .fromQuery(sql).usingStandardSql())
                .apply(SqlTransform.query(
                        "SELECT homeTeamName, `year`, AVG(attendance) avg_attendance FROM PCOLLECTION GROUP BY `year`, homeTeamName")) // year with `` since is part of Calcite SQL
                .apply(BigQueryIO.<Row>write()
                        .withSchema(BigQueryUtils.toTableSchema(tableSchema)) // BigQuerySchema from RowSchema
                        .withFormatFunction(BigQueryUtils.toTableRow()) // TableRow from Row
                        .to(options.getTable())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run();
    }
}

