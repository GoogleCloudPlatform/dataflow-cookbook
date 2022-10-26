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

package extra;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(CustomMetrics.class);

    public interface CustomMetricsOptions extends PipelineOptions {
        @Description("Topic to read from")
        @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
        String getTopic();

        void setTopic(String value);
    }


    public static class MyMetricsDoFn extends DoFn<String, String> {
        private final Counter counterEnroute = Metrics.counter("Message Key", "Enroute");
        private final Counter counterDropoff = Metrics.counter("Message Key", "Dropoff");
        private final Counter counterPickup = Metrics.counter("Message Key", "Pickup");

        // Measures the delta between people in the taxi now and people in the taxi at pipeline start time
        private final Counter counterPeopleInTaxi = Metrics.counter("People", "Delta people in taxi");

        // Measures the distribution of passengers
        private final Distribution costDistribution = Metrics.distribution("People", "Cost (x1000)");

        @ProcessElement
        public void processElement(ProcessContext c) {
            JSONObject json = new JSONObject(c.element());

            String rideStatus = json.getString("ride_status");
            float cost = json.getFloat("meter_reading");

            if (rideStatus.equals("dropoff")) {
                counterDropoff.inc();
                counterPeopleInTaxi.dec(); // Decrease if people leaves the taxi

                // Distributions use long value, multiplying by 1000 gives you more significant decimals
                long costLong = (long) (cost * 1000);
                costDistribution.update(costLong); // Update distribution
            } else if (rideStatus.equals("enroute")) {
                counterEnroute.inc();
            } else if (rideStatus.equals("pickup")) {
                counterPickup.inc();
                counterPeopleInTaxi.inc(); // Increase if people jumps into the taxi
            }

            c.output(c.element());
        }
    }


    public static void main(String[] args) {

        CustomMetricsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomMetricsOptions.class);

        Pipeline p = Pipeline.create(options);

        p.apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
                .apply("Metrics", ParDo.of(new MyMetricsDoFn()));

        p.run();
    }
}
