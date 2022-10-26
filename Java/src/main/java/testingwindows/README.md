## Notes

In order for the windows to take effect, you need to have aggregations or `StatefulDoFns`.

1 - Pipelines need to be ran locally. For example:

```
mvn compile -e exec:java  -Dexec.mainClass=testingWindows.ElementCountTrigger
```

2 - At the end of each file there's an explanation of the output