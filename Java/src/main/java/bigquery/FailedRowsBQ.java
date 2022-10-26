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
import com.google.api.services.bigquery.model.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class FailedRowsBQ {

    private static final Logger LOG = LoggerFactory.getLogger(FailedRowsBQ.class);

    public interface FailedRowsBQOptions extends PipelineOptions {
        @Description("Table to write to")
        String getTable();

        void setTable(String value);

        @Description("Topic to read from")
        @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        String getTopic();

        void setTopic(String value);
    }

    public static void main(String[] args) {

        FailedRowsBQOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FailedRowsBQOptions.class);

        Pipeline p = Pipeline.create(options);

        // SCHEMA IS WRONG so the insert fails
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("ride_status").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        WriteResult inserts =
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
                        // to BigQuery that would fail;
                        .apply("WriteInBigQuery", BigQueryIO.writeTableRows().to(options.getTable())
                                .withSchema(schema)
                                .withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry())  // we don't want retries
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withExtendedErrorInfo()); // Adds info to the error

        // Reference to failed inserts (also works with getFailedInserts() )
        inserts.getFailedInsertsWithErr()  // `withExtendedErrorInfo` in `write` needed
                .apply("Failed rows", ParDo.of(new DoFn<BigQueryInsertError, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws IOException {
                        TableRow row = c.element().getRow();
                        TableDataInsertAllResponse.InsertErrors error = c.element().getError();
                        TableReference table = c.element().getTable();
                        LOG.error("Failed to write row " + row.toString()
                                + ".\n Table " + table.getDatasetId() + "." + table.getTableId()
                                + ".\n Error: " + error.toString()
                        );

                        // Output TableRow
                        c.output(row);
                    }
                })); // Add dead letter here

        p.run();
    }
}
