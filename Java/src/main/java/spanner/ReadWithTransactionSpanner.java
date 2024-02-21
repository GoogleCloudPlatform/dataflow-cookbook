/*
 * Copyright 2024 Google LLC
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
package spanner;

import java.util.Objects;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadWithTransactionSpanner {

    private static final Logger LOG = LoggerFactory.getLogger(ReadWithTransactionSpanner.class);

    public static final String ID_COLUMN = "id";
    public static final String NAME_COLUMN = "name";

    /** Pipeline options for read from Spanner. */
    public interface ReadSpannerOptions extends DataflowPipelineOptions {
        @Description("Google Cloud Spanner instance ID")
        @Validation.Required
        String getInstanceId();

        void setInstanceId(String instanceId);

        @Description("Google Cloud Spanner database ID")
        @Validation.Required
        String getDatabaseId();

        void setDatabaseId(String databaseId);

        @Description("Google Cloud Spanner table name")
        @Validation.Required
        String getTableName();

        void setTableName(String tableName);

        @Description("Google Cloud project ID")
        @Validation.Required
        String getProjectId();

        void setProjectId(String projectId);
    }

    public static void main(String[] args) {
        ReadSpannerOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadSpannerOptions.class);

        Pipeline p = Pipeline.create(options);

        SpannerConfig spannerConfig = SpannerConfig.create()
            .withProjectId(options.getProjectId())
            .withInstanceId(options.getInstanceId())
            .withDatabaseId(options.getDatabaseId());
        PCollectionView<Transaction> tx =
            p.apply(
                "Create transaction",
                SpannerIO.createTransaction()
                    .withSpannerConfig(spannerConfig)
                    .withTimestampBound(TimestampBound.strong()));

        p.apply(
            "Read from Spanner",
            SpannerIO.read()
                .withSpannerConfig(spannerConfig)
                .withTable(options.getTableName())
                .withColumns(ID_COLUMN, NAME_COLUMN)
                .withHighPriority()
                .withTransaction(tx))
            .apply(
                "Log Data",
                ParDo.of(
                    new DoFn<Struct, Struct>() {
                        @DoFn.ProcessElement
                        public void processElement(ProcessContext c) {
                            Struct record = Objects.requireNonNull(c.element());
                            LOG.info(
                                "Id = {}, Name = {}",
                                record.getLong(ID_COLUMN),
                                record.getString(NAME_COLUMN));
                            c.output(record);
                        }
                    }));
        p.run();
    }
}
