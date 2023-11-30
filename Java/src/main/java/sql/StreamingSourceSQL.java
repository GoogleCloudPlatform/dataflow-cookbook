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

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogPipelineOptions;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogTableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingSourceSQL {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingSourceSQL.class);

  public static final Schema rowSchema =
      Schema.builder()
          .addStringField("name")
          .addInt32Field("year")
          .addStringField("country")
          .build();

  public static void main(String[] args) {

    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

    Pipeline p = Pipeline.create(options);

    try (final DataCatalogTableProvider tableProvider =
        DataCatalogTableProvider.create(options.as(DataCatalogPipelineOptions.class))) {

      // List how many passengers were dropped every 10 seconds
      PCollection<Row> rows =
          p.apply(
              SqlTransform.query(
                      "SELECT TUMBLE_START(ride.event_timestamp, INTERVAL '10' SECOND) as period_start, SUM(passenger_count) as `all_passengers` \n"
                          + "FROM pubsub.topic.`pubsub-public-data`.`taxirides-realtime` ride\n"
                          + "WHERE ride_status = 'dropoff' "
                          + "GROUP BY TUMBLE(ride.event_timestamp, INTERVAL '10' SECOND)\n")
                  .withDefaultTableProvider("datacatalog", tableProvider));

      rows.apply(
          "Log",
          ParDo.of(
              new DoFn<Row, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  Row row = c.element();
                  LOG.info(
                      "{} passengers were dropped off at {}",
                      row.getInt64("all_passengers"),
                      row.getDateTime("period_start"));
                }
              }));
      p.run();
    }
  }
}
