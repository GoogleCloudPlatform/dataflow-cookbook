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
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class StreamingInsertsBQ {

    // Parameter parser
    public interface SteamingInsertsBQOptions extends DataflowPipelineOptions {
        @Description("Table to write to")
        String getTable();

        void setTable(String value);

        @Description("Topic to read from")
        @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {
        // Reference the extended class
        SteamingInsertsBQOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SteamingInsertsBQOptions.class);

        Pipeline p = Pipeline.create(options);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("ride_status").setType("STRING"));
        fields.add(new TableFieldSchema().setName("passenger_count").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("meter_reading").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
        TableSchema schema = new TableSchema().setFields(fields);

        p
                .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
                //Convert to Row
                .apply("Convert To Row", ParDo.of(new DoFn<String, TableRow>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws ParseException {
                                JSONObject json = new JSONObject(c.element());

                                String rideStatus = json.getString("ride_status");
                                Integer passengerCount = json.getInt("passenger_count");
                                Float meterReading = json.getFloat("meter_reading");
                                String timestamp = json.getString("timestamp");

                                TableRow row = new TableRow();

                                row.set("ride_status", rideStatus);
                                row.set("passenger_count", passengerCount);
                                row.set("meter_reading", meterReading);
                                row.set("timestamp", timestamp);

                                c.output(row);
                            }
                        })
                )
                .apply("WriteInBigQuery", BigQueryIO.writeTableRows().to(options.getTable())
                        .withSchema(schema)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())  // only available for Streaming
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        p.run();
    }
}