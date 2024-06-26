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
import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadStorageAPIBQ {
  private static final Logger LOG = LoggerFactory.getLogger(ReadStorageAPIBQ.class);

  // Parameter parser
  public interface ReadStorageAPIBQOptions extends PipelineOptions {
    @Description("Table to read")
    @Default.String("bigquery-public-data:sec_quarterly_financials.calculation")
    String getTable();

    void setTable(String value);
  }

  public static void main(String[] args) {
    // Reference the extended class
    ReadStorageAPIBQOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadStorageAPIBQOptions.class);

    Pipeline p = Pipeline.create(options);

    p.apply(
            "BQ Storage API",
            BigQueryIO.readTableRows()
                .from(options.getTable())
                .withMethod(Method.DIRECT_READ) // Storage API
                .withRowRestriction(
                    "DATE(_PARTITIONTIME) > \"2019-01-01\" AND parent_tag=\"Assets\"") // Filter
                                                                                       // (like a
                                                                                       // WHERE)
                .withSelectedFields(
                    Lists.newArrayList(
                        "submission_number", "group", "arc")) // Selecting only some fields
            )
        .apply(
            ParDo.of(
                new DoFn<TableRow, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().toString());
                  }
                }));

    p.run();
  }
}
