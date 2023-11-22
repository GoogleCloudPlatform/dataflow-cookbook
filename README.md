# Dataflow Cookbook

## Goal

The goal of the cookbook is to provide ready-to-launch and selfcontained
pipelines so that creating new pipelines becomes easier. The examples in the
cookbook are the most common use cases when using Dataflow.

When possible, the pipeline parameters are prepopulated to include public
resources in order for the pipelines to be as easy to execute as possible. When
actions are required from the user, there should be a pipeline parameter option
for you to fill and / or a comment stating that the pipeline needs preparation,
for example
`Java/gcs/MatchAllContinuouslyFileIO`.

## Content

The cookbook contains examples for Java, Python and Scala.

### Java

- *basics*

- *gcs*

- *bigquery*

- *pubsub*

- *pubsublite*

- *sql*: pipelines that use BeamSQL

- *cloudsql*

- *windows*: example pipelines of the four windows

- *testingWindows*: pipelines that, using the way to test windows, explain how
    triggers work (`AfterEach`, `AfterFirst` and so on). These can be used to
    learn triggers.

- *advanced*: pipelines with not so usual use cases, such as custom windows,
    Stateful and Timer DoFns.

- *minimal*: minimal pipelines that can be used to start a custom one.

- *bigtable*

- *spanner*

- *datastore*

- *kafka*

- *extra*: what could not fit in the other section.

### Python

- *basics*

- *gcs*

- *json*

- *bigquery*

- *kafka*

- *pubsub*

- *spanner*

- *tfrecord*

- *windows*: example pipelines of the four windows

- *testing_windows*: pipelines that, using the way to test windows, explain how
  triggers work. These can be used to learn triggers.

- *minimal*: minimal pipelines that can be used to start a custom one.

- *advanced*: pipelines with not so usual use cases, such as Timely and Stateful
  DoFn examples

- *extra_examples*: what could not fit in the other section.

### Scala / Scio

- *basics*

- *gcs*

- *bigquery*

- *pubsub*

- *windows*: example pipelines of the four windows

- *minimal*: minimal pipelines that can be used to start a custom one.

- *advanced*: pipelines with not so usual use cases, such as Timely and Stateful
  DoFn examples

- *extra*: what could not fit in the other section.

## Setting up the environment

- (Optional for Python) Download and set your credentials as
  [documented](https://cloud.google.com/docs/authentication/getting-started).

- Set up environment variables. In the terminal, run (change values between
  < >)

  ```
  export BUCKET=<YOUR_BUCKET_NAME>
  export REGION=<YOUR_REGION>
  ```

- For Python, you can also set the project
  variable: `export PROJECT=<YOUR_PROJECT>`

## Launching Dataflow Jobs

### Java

To launch the dataflow jobs run in your terminal (using `basics/groupByKey` as
example):

```
mvn compile -e exec:java -Dexec.mainClass=basics.groupByKey \
-Dexec.args="--runner=DataflowRunner --region=$REGION \
--tempLocation=gs://$BUCKET/tmp/"
```

In some pipelines you would need to add arguments, for example in
`bigquery.WriteDynamicBQ` you need to add a dataset:

```
mvn compile -e exec:java -Dexec.mainClass=bigquery.WriteDynamicBQ \
-Dexec.args="--runner=DataflowRunner --region=$REGION \
--tempLocation=gs://$BUCKET/tmp/ --dataset=$DATASET"
```

The extra parameters needed can be seen in the pipeline code, checking the
pipeline options.

### Python

To launch the dataflow jobs run in your terminal (using `basics/group_by_key.py`
as example):

```
python group_by_key.py --runner DataflowRunner --project $PROJECT \
--region $REGION --temp_location gs://$BUCKET/tmp/
```

In some pipelines you would need to add arguments, for example in
`bigquery/write_bigquery.py` you need to add a output table:

```
python write_bigquery.py --runner DataflowRunner --project $PROJECT \
--region $REGION --temp_location gs://$BUCKET/tmp/ --output_table $MY_TABLE
```

The extra parameters needed can be seen in the pipeline code, checking the
pipeline options class.

NOTE: If you want to name the pipeline, add `--job_name=my-pipeline-name`.

### Scala / SCio

To launch the dataflow jobs run in your terminal (using `basics/GroupByKey` as
example):

```
sbt "runMain basics.GroupByKey --runner=DataflowRunner --region=$REGION \
--tempLocation=gs://$BUCKET/tmp/"
```

In some pipelines you would need to add arguments, for example in
`bigquery/WriteStreamingInserts` you need to add a table:

```
sbt "runMain bigquery.WriteStreamingInserts --runner=DataflowRunner --region=$REGION \
--tempLocation=gs://$BUCKET/tmp/ --table=$MY_TABLE"
```

The extra parameters needed can be seen in the pipeline code, checking for
`opts` or `opts.getOrElse`.