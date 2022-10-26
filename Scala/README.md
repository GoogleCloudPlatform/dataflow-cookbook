## SCio

**SCio** is a wrap up of *Apache Beam* for **Scala**, made by Spotify.

In order to use this cookbook, you need to have Scala in your computer.

To launch the pipelines, run commands like this from the root:

`sbt "runMain basics.Join"`

If you want to use Dataflow, you need to add your [credentials](https://cloud.google.com/docs/authentication/getting-started)
and run:

```
export BUCKET=<YOUR_BUCKET_NAME>
export REGION=<YOUR_REGION>
export PROJECT=<YOUR_PROJECT>
sbt "runMain basics.Join --project=$PROJECT --region=$REGION --runner=DataflowRunner --tempLocation=gs://$BUCKET/tmp/"
```

Find more documentation here:

- https://spotify.github.io/scio/index.html
- https://github.com/spotify/scio