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

package advanced;

import static com.google.common.base.MoreObjects.firstNonNull;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerDoFn {

  private static final Logger LOG = LoggerFactory.getLogger(TimerDoFn.class);

  // Parameter parser
  public interface TimerDoFnOptions extends DataflowPipelineOptions {
    @Description("Table to write to")
    String getTable();

    void setTable(String value);

    @Description("Topic to read from")
    @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
    String getTopic();

    void setTopic(String value);
  }

  public static void main(String[] args) throws MalformedURLException {
    TimerDoFnOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TimerDoFnOptions.class);

    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("ride_status").setType("STRING"));
    fields.add(new TableFieldSchema().setName("meter_reading").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
    fields.add(new TableFieldSchema().setName("passenger_count").setType("INTEGER"));
    TableSchema schema = new TableSchema().setFields(fields);

    Pipeline p = Pipeline.create(options);

    p.apply("Read From PubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
        .apply(
            "Parse and to KV",
            ParDo.of(
                new DoFn<String, KV<String, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws ParseException {
                    JSONObject json = new JSONObject(c.element());
                    String rideStatus = json.getString("ride_status");

                    c.output(KV.of(rideStatus, c.element()));
                  }
                }))
        // Stateful DoFn need to have a KV as input
        .apply(
            "Timer",
            ParDo.of(
                new DoFn<KV<String, String>, TableRow>() {
                  // GroupIntoBatches PTransfrom implements a similar logic

                  private final Duration BUFFER_TIME = Duration.standardSeconds(180);

                  @TimerId("timer")
                  private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

                  @StateId("counter")
                  // Count of elements in buffer
                  private final StateSpec<ValueState<Integer>> counter = StateSpecs.value();

                  @StateId("buffer")
                  // Elements will be saved here, with type TableRow
                  private final StateSpec<BagState<TableRow>> bufferedEvents = StateSpecs.bag();

                  @ProcessElement
                  public void processElement(
                      ProcessContext c,
                      @TimerId("timer") Timer timer,
                      @StateId("counter") ValueState<Integer> counter,
                      @StateId("buffer") BagState<TableRow> buffer)
                      throws ParseException {

                    int count = firstNonNull(counter.read(), 0);

                    if (count == 0) {
                      timer.offset(BUFFER_TIME).setRelative(); // only offsetting if trigger is 0
                      LOG.info("TIMER: Setting timer at " + Instant.now().toString());
                    }

                    JSONObject json = new JSONObject(c.element().getValue());

                    String timestamp = json.getString("timestamp");
                    Float meterReading = json.getFloat("meter_reading");
                    Integer passengerCount = json.getInt("passenger_count");

                    TableRow row = new TableRow();

                    row.set("ride_status", c.element().getKey());
                    row.set("timestamp", timestamp);
                    row.set("meter_reading", meterReading);
                    row.set("passenger_count", passengerCount);

                    buffer.add(row); // add to buffer the row
                    count = count + 1;
                    counter.write(count); // overwrite trigger
                  }

                  // This method is call when timers expire
                  @OnTimer("timer")
                  public void onTimer(
                      OnTimerContext c,
                      @StateId("counter") ValueState<Integer> counter,
                      @StateId("buffer") BagState<TableRow> buffer)
                      throws IOException {
                    LOG.info(
                        "TIMER: Releasing buffer at "
                            + Instant.now().toString()
                            + " with "
                            + counter.read().toString()
                            + " elements");
                    for (TableRow row : buffer.read()) { // for every element in buffer
                      c.output(row);
                    }
                    counter.clear(); // resetting counter so we offset in "process"
                    buffer.clear(); // clearing buffer
                  }
                }))
        .apply(
            "WriteInBigQuery",
            BigQueryIO.writeTableRows()
                .to(options.getTable())
                .withSchema(schema)
                .withFailedInsertRetryPolicy(
                    InsertRetryPolicy.neverRetry()) // only available for Streaming
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    p.run();
  }
}
