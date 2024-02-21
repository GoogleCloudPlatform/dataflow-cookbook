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
import org.apache.beam.sdk.transforms.Create;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;

public class WriteMongoDB {

    private static final String ID_COLUMN = "id";
    private static final String NAME_COLUMN = "name";

    /**
     * Pipeline options for write to MongoDB.
     */
    public interface WriteMongoDbOptions extends PipelineOptions {
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
        WriteMongoDbOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation().as(WriteMongoDbOptions.class);

        Pipeline p = Pipeline.create(options);

        List<Document> rows = Arrays.asList(
                new Document(ID_COLUMN, 1).append(NAME_COLUMN, "Charles"),
                new Document(ID_COLUMN, 2).append(NAME_COLUMN, "Alice"),
                new Document(ID_COLUMN, 3).append(NAME_COLUMN, "Bob"),
                new Document(ID_COLUMN, 4).append(NAME_COLUMN, "Amanda"),
                new Document(ID_COLUMN, 5).append(NAME_COLUMN, "Alex"),
                new Document(ID_COLUMN, 6).append(NAME_COLUMN, "Eliza")
        );

        p.apply("Create", Create.of(rows))
            .apply(
                "Write to MongoDB",
                    MongoDbIO.write()
                            .withUri(options.getUri())
                            .withDatabase(options.getDbName())
                            .withCollection(options.getCollection()));

        p.run();
    }
}
