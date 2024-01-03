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

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadQueryBQ {

    private static final Logger LOG = LoggerFactory.getLogger(ReadQueryBQ.class);

    public static void main(String[] args) {

        // Parse pipeline options from the command line.
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        String sql = "SELECT " +
                "homeTeamName, awayTeamName, AVG(attendance) avgAttendance " +
                "FROM `bigquery-public-data.baseball.schedules` " +
                "GROUP BY 1, 2";

        // Construct the pipeline.
        Pipeline p = Pipeline.create(options);

        p
                // Add a source that reads the query results from BigQuery.
                .apply(BigQueryIO.readTableRows().fromQuery(sql).usingStandardSql())
                // Add a transform that filters elements by attendance and prints the results.
                .apply("Access TableRow", ParDo.of(new DoFn<TableRow, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        TableRow row = c.element();
                        String local = row.get("homeTeamName").toString();
                        String away = row.get("awayTeamName").toString();
                        String attendance = row.get("avgAttendance").toString();

                        if (Double.parseDouble(attendance) > 30000) {
                            String headline = "Match between (local) " + local + " and " + away + " has an average attendance of " + attendance;
                            LOG.info(headline);
                            c.output(headline);
                        }
                    }
                }));

        // Execute the pipeline.
        p.run();
    }
}
