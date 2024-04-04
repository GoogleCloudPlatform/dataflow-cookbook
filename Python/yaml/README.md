## Running pipelines

The Beam yaml parser is currently included as part of the Apache Beam Python SDK.
This can be installed (e.g. within a virtual environment) as

```
pip install apache_beam[yaml,gcp]
```

In addition, several of the provided transforms (such as SQL) are implemented
in Java and their expansion will require a working Java interpeter. (The
requisite artifacts will be automatically downloaded from the apache maven
repositories, so no further installs will be required.)
Docker is also currently required for local execution of these
cross-language-requiring transforms, but not for submission to a non-local
runner such as Flink or Dataflow.

Once the prerequisites are installed, you can execute a pipeline defined
in a yaml file as

```
python -m apache_beam.yaml.main --yaml_pipeline_file=/path/to/pipeline.yaml [other pipeline options such as the runner]
```