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
package mongodb;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadMongoDB {

    private static final Logger LOG =
            LoggerFactory.getLogger(ReadMongoDB.class);

    private static final String ID_COLUMN = "id";
    private static final String NAME_COLUMN = "name";

    /**
     * Pipeline options for read from MongoDB.
     */
    public interface ReadMongoDbOptions extends PipelineOptions {
        @Description("The MongoDB connection string following the URI format")
        @Default.String("mongodb://localhost:27017")
        String getUri();

        void setUri(String uri);

        @Description("The MongoDB database name")
        @Validation.Required
        String getDbName();

        void setDbName(String dbName);

        @Description("The MongoDB collection name")
        @Validation.Required
        String getCollection();

        void setCollection(String collection);
    }

    public static void main(String[] args) {
        ReadMongoDbOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation().as(ReadMongoDbOptions.class);

        Pipeline p = Pipeline.create(options);

        p.apply(
                "Read from MongoDB",
                MongoDbIO.read()
                    .withUri(options.getUri())
                    .withDatabase(options.getDbName())
                    .withCollection(options.getCollection()))
            .apply(
                "Log Data",
                ParDo.of(
                    new DoFn<Document, Document>() {
                        @DoFn.ProcessElement
                        public void processElement(ProcessContext c) {
                            LOG.info(
                                "Id = {}, Name = {}",
                                c.element().get(ID_COLUMN),
                                c.element().get(NAME_COLUMN));
                            c.output(c.element());
                        }
                    }));

        p.run();
    }
}