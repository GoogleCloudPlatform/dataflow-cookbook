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
package bigtable;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline for reading data from Google Cloud Bigtable with row filtering and key range
 * specification. The pipeline uses the {@code BigtableIO.read()} transform to read rows from a
 * specified table in a Cloud Bigtable instance, applying a custom row filter based on a regular
 * expression to selectively exclude rows. The {@code withKeyRange()} method is used to limit the
 * scope of keys read from the Cloud Bigtable table, allowing for more targeted data retrieval.
 */
public class ReadWithRowFilterBigTable {

    private static final Logger LOG = LoggerFactory.getLogger(ReadWithRowFilterBigTable.class);
    private static final String DATE_TIME_PATTERN = "yyyy/MM/dd HH:mm:ss";
    private static final String EXAMPLE_REGEX = ".*a.*";
    private static final String START_KEY = "beam_key0000001";
    private static final String END_KEY = "beam_key0000004";

    /** Pipeline options for read from BigTable. */
    public interface ReadBigTableOptions extends PipelineOptions {
        @Description("Project ID")
        @Validation.Required
        String getProjectId();

        void setProjectId(String projectId);

        @Description("Cloud Bigtable instance ID")
        @Validation.Required
        String getInstanceId();

        void setInstanceId(String instanceId);

        @Description("Cloud Bigtable table ID")
        @Validation.Required
        String getTableId();

        void setTableId(String tableId);
    }

    public static void main(String[] args) {
        ReadBigTableOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadBigTableOptions.class);

        Pipeline p = Pipeline.create(options);

        RowFilter rowFilter =
            RowFilter.newBuilder().setValueRegexFilter(ByteString.copyFromUtf8(EXAMPLE_REGEX)).build();

        ByteKey startKey = ByteKey.copyFrom(START_KEY.getBytes(StandardCharsets.UTF_8));
        ByteKey endKey = ByteKey.copyFrom(END_KEY.getBytes(StandardCharsets.UTF_8));
        final ByteKeyRange keyRange = ByteKeyRange.of(startKey, endKey);

        p.apply(
                "Read from BigTable",
                BigtableIO.read()
                    .withProjectId(options.getProjectId())
                    .withInstanceId(options.getInstanceId())
                    .withTableId(options.getTableId())
                    .withRowFilter(rowFilter)
                    .withKeyRange(keyRange))
            .apply("Log Data", ParDo.of(new FormatDoFn()));
        p.run();
    }

    /**
     * Custom {@link DoFn} designed to format a BigTable {@link Row} into a human-readable string
     * representation for logging or output.
     */
    private static class FormatDoFn extends DoFn<Row, String> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            Row row = Objects.requireNonNull(c.element());
            DateTimeFormatter format = DateTimeFormat.forPattern(DATE_TIME_PATTERN);
            StringBuilder rowString = new StringBuilder("Row: " + row.getKey().toStringUtf8());

            for (Family family : row.getFamiliesList()) {
                StringBuilder familyString =
                    new StringBuilder("\t Family: ").append(family.getName());
                for (Column column : family.getColumnsList()) {
                    StringBuilder columnString =
                        new StringBuilder("\t\t Column: ").append(column.getQualifier().toStringUtf8());
                    for (Cell cell : column.getCellsList()) {
                        String timestamp =
                            Instant.ofEpochMilli(cell.getTimestampMicros() / 1000).toString(format);
                        columnString
                            .append("\n")
                            .append("\t\t\t Cell: ")
                            .append(cell.getValue().toStringUtf8())
                            .append(" at ")
                            .append(timestamp);
                    }
                    familyString.append("\n").append(columnString);
                }
                rowString.append("\n").append(familyString);
            }
            LOG.info(rowString.toString());
            c.output(rowString.toString());
        }
    }
}
