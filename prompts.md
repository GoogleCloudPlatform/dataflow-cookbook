# Prompts (01-23-2024)

|Prompt|Path|
|---|---|
|How do I contribute to this Dataflow Cookbook project?|CONTRIBUTING.md|
|Write an Apache Beam Python code to count the total number of lines for text files in GCS.|Python/gcs/read_all_textio.py|
|Write me a Beam Python code example to demonstrate how to read messages from the Pub/Sub topic "projects/pubsub-public-data/topics/taxirides-realtime" and process them with a simple aggregation example.|Python/windows/fixed_windows.py|
|Write an Apache Beam Python code to demonstrate how to use the Beam TestStream  utility to test the windowing operations.|Python/testing_windows/accumulating_fired_panes.py|
|Write an Apache Beam Python code to use the  “ReadAllFromBigQuery” transform to define the table and query reads from BigQuery at pipeline runtime.|Python/bigquery/read_all_bigquery.py|
|Write a Beam Python code to demonstrate how to write data into different BigQuery tables based on the data.|Python/bigquery/write_bigquery_dynamic.py|
|Write a Beam Python code to demonstrate how to write data into a BigQuery table.|Python/bigquery/write_bigquery.py|
|Write a Python code using Apache Beam to demonstrate how to use Combiners from Beam such as Count, Mean, Top|Python/basics/builtin_combiners.py|
|Create one example for a Apache Beam streaming pipeline, which reads messages from Pub/Sub,  apply a session window of 120 seconds and CombinePerKey for some simple aggregations.|Python/windows/session_window.py|
|Write an Apache Beam Python code to read multiple Pub/Sub topics or subscriptions and log the messages.|Python/pubsub/read_pubsub_multiple.py|
|Write an Apache Beam Python code to split a PCollection into two PCollections based on even/odd numbers.|Python/basics/pardo_with_output.py|
|Write an Apache Beam Python code to write data into BigQuery table.|Python/bigquery/write_bigquery.py|
|Write an Apache Beam Python code to write data to a file system.|Python/extra_examples/file_system_dynamics.py|
|Write an Apache Beam Python pipeline that reads data from a BigQuery table and logs the data.|Python/bigquery/read_table_ref_bigquery.py|
|Write an Apache Beam Python pipeline to count the number of lines in a GCS file.|Python/gcs/read_textio.py|
|Write an Apache Beam Python pipeline to read data from BigQuery using a SQL query and log the results.|Python/bigquery/read_query_bigquery.py|
|Write a Beam Java pipeline that starts with a fixed set of elements.|Java/src/main/java/minimal/create.java|
|Write a Beam Java pipeline that uses a query to query data from Spanner.|Java/src/main/java/spanner/ReadQuerySpanner.java|
|Generate a Apache Beam pipeline code to read data from Pub/Sub subscription.|Java/src/main/java/pubsub/ReadSubscriptionPubSub.java|
|Write an Apache Beam Dataflow pipeline to read from Cloud Datastore.|Java/src/main/java/datastore/ReadDatastore.java|
|Write an Apache Beam code to consume data from Pub/Sub and calculate the total number of passengers per ride status in a sliding window.|Java/src/main/java/windows/SlidingWindow.java|
|Write an Apache Beam code to convert amounts in different currencies to USD. Use a side input to store the conversion rates.|Java/src/main/java/basics/SideInputs.java|
|Write an Apache Beam Java Pipeline to read from a BigQuery table, convert each row to a string and output.|Java/src/main/java/bigquery/ReadTableBQ.java|
|Write an Apache Beam Java pipeline to write 100 lines to a text file.|Java/src/main/java/gcs/WriteTextIO.java|
|Write an Apache Beam Java pipeline to write data to dynamic table destinations in BigQuery.|Java/src/main/java/bigquery/WriteDynamicBQ2.java|
|Write an Apache Beam Java pipeline to write data to Pub/Sub Lite.|Java/src/main/java/pubsublite/WritePubSubLite.java|
|Write an Apache Beam Java pipeline to write data to different Avro files dynamically.|Java/src/main/java/gcs/WriteAvroDynamic.java|
|Write an Apache Beam Java pipeline to write 100 integers to the main PCollection and three additional PCollections based on whether the number is a multiple of 3, 5, or 15. |Java/src/main/java/basics/ParDoWithOutputs.java|
|Write an Apache Beam Java pipeline to read an Avro file from GCS and log the contents.|Java/src/main/java/gcs/ReadGenericAvroIO.java|
|Write an Apache Beam pipeline to read from Pub/Sub topic and write to another Pub/Sub topic.|Python/pubsub/write_pubsub.py|
|Write an Apache Beam Python pipeline to write data to Avro format.|Python/gcs/write_avro.py|
|Write an Apache Beam Python code to read data from Pub/Sub subscription and print the message.|Python/pubsub/read_pubsub_subscription.py|
|Write an Apache Beam pipeline that reads from a Pub/Sub topic, matches all GCS paths in the messages, reads the files, counts the lines in the files per window and logs the counts.|Java/src/main/java/gcs/MatchAllFileIOStreaming.java|
|Write an Apache Beam pipeline to consume messages from Pub/Sub, apply a sliding window and compute the sum of the passenger_count per ride_status. Log the results.|Python/windows/sliding_window.py|
|Write an Apache Beam pipeline to load data from Pub/Sub, select the fields ride_status, passenger_count, meter_reading and timestamp, then write the data to BigQuery.|Python/bigquery/streaming_load_jobs_bigquery.py|
|Write a python pipeline using Apache Beam that prints the numbers from 0 to 9.|Python/minimal/create.py|
|Write a python code using Apache Beam that groups elements by their keys from two PCollections.|Python/basics/co_group_by_key.py|
|Write a python code using Apache Beam that groups elements by their keys and log the output.|Python/basics/group_by_key.py|
|Write an Apache Beam Java Pipeline to execute a query in BigQuery, read the records, filter and transform them, and output the results.|Java/src/main/java/bigquery/ReadQueryBQ.java|
|Write an Apache Beam Java Pipeline that writes string messages to Pub/Sub.|Java/src/main/java/pubsub/WriteStringPubSub.java|
|Write an Apache Beam Java Pipeline that writes records to a BigQuery table.|Java/src/main/java/bigquery/WriteBQ.java|
|Write an Apache Beam Java Pipeline that writes integers to Bigtable splitting them into "prime" and "not-prime" Bigtable families depending on whether one is a prime number.|Java/src/main/java/bigtable/WriteBigTable.java|
|Write an Apache Beam Java Pipeline that writes messages with attributes to Pub/Sub.|Java/src/main/java/pubsub/WriteWithAttributesPubSub.java|
|Write a Beam SQL pipeline to read a table from BigQuery, apply a SQL query and write the result back to BigQuery.|Java/src/main/java/sql/BigQuerySQL.java|
|Write a Beam SQL streaming pipeline in Java to read data from Pub/Sub and involve windowing features.|Java/src/main/java/sql/WindowingSQL.java|
|Write an Apache Beam pipeline to print the list of available SSL ciphers.|Java/src/main/java/extra/SSLCiphers.java|
|Write a Beam pipeline to read a table from BigQuery with Storage API then filter the rows by date and parent_tag and select only some fields.|Java/src/main/java/bigquery/ReadStorageAPIBQ.java|
|Write an Apache Beam pipeline to read a table from BigQuery, modify a nested field and write the results back to BigQuery.|Python/bigquery/repeated_bigquery.py|
|Write a Apache Beam pipeline that reads data from a TestStream. Add four elements at timestamps 0, 4, 6 and 5. Then advance the watermark to 9 and advance the processing time to 9. Then add an element at timestamp 0. Then advance the watermark to 11 and processing time to 3. Add an element at timestamp 9, advance the watermark to 15 and processing time to 4. Add elements at timestamp 9 and timestamp 11. Finally advance the watermark to infinity. Assign elements within the same pane with the same key, windows into FixedWindows with allowed lateness, groups together elements within same pane and key, and formats the elements. Use a fixed window with allowed lateness and a discarding accumulation mode.|Python/testing_windows/late_data.py|
|Write a Apache Beam pipeline that reads from a Pub/Sub topic, that attempts to assign windows based on the element’s content, and then samples the output on a fixed size per key basis; fails to emit elements and does not perform null checks|Java/src/main/java/advanced/CustomWindows.java|
|Write an Apache Beam pipeline to read data from Bigtable. Also output as comments how to provision minimally required dependent resources and submit as a Dataflow Job.|Java/src/main/java/bigtable/ReadBigTable.java|
|Write a Beam pipeline with intent to read  from Pub/Sub and write to BigQuery that raises “TypeError: expected string or buffer”. |Python/bigquery/failed_rows_bigquery.py|
