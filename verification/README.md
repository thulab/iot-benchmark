Benchmark for Verification and Double-Write
---

# 1. Correctness Verification

## 1.1. Environment
Correctness verification currently supports only IoTDB v1.0 and later, and TimescaleDB.

## 1.2. Related Modes
1. `testWithDefaultPath`: verify correctness after the benchmark generates and writes the dataset.
2. `generateDataMode`: generate the local dataset used for verification.
3. `verificationWriteMode`: double-write the locally generated dataset into two different databases.
4. `verificationQueryMode`: perform point-by-point queries based on the local dataset to confirm data correctness in the databases.

## 1.3. Verification Methods

### 1.3.1. Verify Without Recording the Dataset
> Related benchmark mode: `testWithDefaultPath`

1. First, use the benchmark to write data into two databases. The related configuration file is [write config](conf/write.properties).
2. Then, use the benchmark to query and compare the two databases. The related configuration file is [query config](conf/query.properties).

### 1.3.2. Verify After Recording the Dataset
> Related benchmark modes: `generateDataMode`, `verificationWriteMode`, `verificationQueryMode`

1. First, use `generateDataMode` to generate the local dataset. The related configuration file is [generate config](conf/generate.properties).
2. Then, use `verificationWriteMode` to write the dataset into two databases. The related configuration file is [generate write config](conf/generate-write.properties).
3. Finally, use `verificationQueryMode` to query the dataset in the two databases with point queries. The related configuration file is [generate query config](conf/generate-query.properties).

# 2. Double-Write Mode
1. Double-write mode supports comparison only between different databases. It does not support double-writing across different versions of the same database.
2. For InfluxDB, only v2.0 and later are supported.
