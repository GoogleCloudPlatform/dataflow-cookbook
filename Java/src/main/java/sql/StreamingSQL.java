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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.TableView;
import java.text.ParseException;


public class StreamingSQL {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingSQL.class);

    public interface StreamingSQLOptions extends PipelineOptions {
        @Description("Topic to read from")
        @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        String getTopic();

        void setTopic(String value);
    }

    public static final Schema rowSchema = Schema.builder()
            .addStringField("ride_status")
            .addDoubleField("passenger_count")
            .addDoubleField("meter_reading")
            .addDateTimeField("timestamp")
            .build();

    public static void main(String[] args) {

        StreamingSQLOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamingSQLOptions.class);

        Integer windowLength = 10;

        String sql = "SELECT " +
                "COUNT(ride_status) total_rides, " +
                "AVG(meter_reading) avg_cost, " +
                "AVG(passenger_count) avg_passenger_count," +
                "AVG(meter_reading)/AVG(passenger_count) avg_per_passenger, " +
                "MAX(`timestamp`) max_timestamp " +
                "FROM PCOLLECTION " +
                "WHERE ride_status = 'dropoff' ";

        Pipeline p = Pipeline.create(options);


        p
                .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
                // Convert to Row
                .apply(JsonToRow.withSchema(rowSchema))
                // Windows are needed for streaming, since SQL is an aggregation
                // They can be added as normal Beam Windows or with SQL (see WindowingSQL example)
                .apply(Window.<Row>into(
                        FixedWindows.of(Duration.standardMinutes(windowLength)))
                        .withAllowedLateness(Duration.standardMinutes(windowLength / 2))
                        .discardingFiredPanes())
                // SQL
                .apply(SqlTransform.query(sql))
                .apply("Log", ParDo.of(new DoFn<Row, String>() {
                            @ProcessElement
                            public void processElement(@Element Row row, OutputReceiver<String> out) {
                                LOG.info(row.toString());
                                out.output(row.toString());
                            }
                        })
                );


        p.run();
    }
}
