#### How to use rep-compare-branch.sh to compare different branch?
Example
```
./ciscripts/rep-compare-branch.sh fit/ingestion-overflow50-auto-test iotdbconf/fit false master ~/.m2/repository/cn
```

1. The first parameter is to define the workload config file, e.g. ```fit/ingestion-overflow50-auto-test```
2. The second parameter is to define the config of IoTDB, e.g. ```iotdbconf/fit```
3. The third parameter decides whether test baseline IoTDB i.e. v0.7.0, e.g. ```false```
4. The fourth parameter decides which branch to test, e.g. ```master```
5. The fifth parameter is the path of your maven repository where you want to clean up before each new test, e.g.  ```~/.m2/repository/cn```

The results are collected in result.txt