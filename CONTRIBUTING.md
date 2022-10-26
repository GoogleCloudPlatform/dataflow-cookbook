# How to Contribute

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

1 - The pipelines should be as minimal as possible and self-contained.

2 - When possible, use public sources (public PubSub topic, GCS bucket, BQ
table...) . If this is not possible, add a comment explaining the needed
process.

3 - It's OK to add comments as clarifications.

4 - Use logging as a way to show the pipeline is correct, for example, if the
pipeline reads from a source, add a step that handles the read element and log
it. If the amount of logging is considerable, add an aggregation or filter step.

5 - No need to add dead letter paths to the pipeline, unless required by the use
case.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement (CLA). You (or your employer)
retain the copyright to your contribution; this simply gives us permission to
use and redistribute your contributions as part of the project. Head over to
<https://cla.developers.google.com/> to see your current agreements on file or
to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## Code Reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google/conduct/).
