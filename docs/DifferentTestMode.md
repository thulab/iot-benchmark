## 1. Different operation modes of IoT Benchmark

Again, before you start any new test case, you need to confirm whether the configuration information in the configuration file ```config.properties``` meets your expectations.

> Note:
> 1. Specific database modules may not support every operation in `OPERATION_PROPORTION`; check the target module README if needed.

### 1.1. General test mode: write (single database)

Assume that the workload parameters are:

```
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
| Measurement/StorageGroup | tag/device | field/sensor | concurrency | batch size | point interval(ms) | loop |
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
| 10 | 50 | 500 | 20 | 100 | 200 | 10000 |
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
```

Note: The total number of time series in this configuration is: ```device * sensor = 25,000```, and the number of points in each time series is ```batch size * loop = 20,000```,
The total number of data points is ```deivce * sensor * batch size * loop = 500,000,000```. The space occupied by each data point can be estimated as 16 bytes, so the total size of the raw data is 8G.

Then, the ```config.properties``` file needs to be modified as shown below. Please remove the ```#``` before the corresponding configuration item after completing the modification to ensure that the changes take effect:

```properties
HOST=127.0.0.1
PORT=6667
DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
BENCHMARK_WORK_MODE=testWithDefaultPath
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0:0:0
GROUP_NUMBER=10
DEVICE_NUMBER=50
SENSOR_NUMBER=500
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=100
POINT_STEP=200
LOOP=10000
```

If you want to write data in a disorderly manner, you need to modify the following properties of the `config.properties` file:

```
# Whether to write out of order
IS_OUT_OF_ORDER=true
# Out of order write mode, currently there are 2 types
# POISSON Out of order mode according to Poisson distribution
# BATCH Batch insert out of order mode
OUT_OF_ORDER_MODE=BATCH
# The proportion of data written out of order
OUT_OF_ORDER_RATIO=0.5
# Is it equal time stamp
IS_REGULAR_FREQUENCY=true
# Expectation and variance of Poisson distribution
LAMBDA=2200.0
# Maximum value of random number of Poisson distribution model
MAX_K=170000
```

Some basic modification operations of parameters have been introduced here, and the relevant settings will be omitted in the following part. If necessary, please refer to the detailed configuration examples in [DifferenttestModeConfig.md](./DifferenttestModeConfig.md).

## 2. General test mode: query (single database, no system records)

In addition to writing data, the general test mode can also query data.

When there is no write operation in the workload, `IS_DELETE_DATA` and `CREATE_SCHEMA` are not needed, and `IS_RECENT_QUERY` will be disabled automatically.

## 3. General test mode: read-write mixed mode (single database)

General test mode can support users to perform read-write mixed tests. It should be noted that the timestamps of read-write mixed in this scenario all start from the **write start time**.

## 4. General test mode: read-write mixed mode (single database, query the most recently written data)

General test mode can support users to perform read-write mixed tests (query the most recently written data). It should be noted that the query time range in this scenario is the data adjacent to the left of the current maximum write timestamp.

## 5. General test mode: use system records (single database)

IoT Benchmark supports recording system data during the test. In the currently documented workflow, this part uses CSV-based records together with system monitoring.

## 6. Conventional test mode: test process persistence (single database)

For subsequent analysis, IoT Benchmark can persist test information to a backend database such as IoTDB or MySQL (if you do not want to store test data, set ```TEST_DATA_PERSISTENCE=None```).

## 7. Generate data mode

In order to generate reusable data sets, IoT Benchmark provides a mode for generating data sets, generating data sets to FILE_PATH for subsequent use in correctness write mode and correctness query mode.

## 8. Correctness write mode (single database, external data set)

In order to verify the correctness of data set writing, you can use this mode to write the data set generated in generate data mode back into the target database. This mode is also used in dataset-based verification workflows.

## 9. Correctness single-point query mode (single database, external data set)

Before running this mode, you need to use the correctness write mode to write data to the database. To verify the correctness of the written data set, you can use `verificationQueryMode` to query the data set written to the database. According to the current code and module support, this mode is available in modules including IoTDB v1.0 and later, InfluxDB v1.x, TimescaleDB, and CnosDB.

## 10. Dual database mode

To complete the correctness verification more conveniently and quickly, IoT Benchmark also supports dual database mode.

1. For all the test scenarios mentioned above, unless otherwise specified, dual databases are supported. Please **start the test** in the `verification` project.
2. `IS_COMPARISON` and `IS_POINT_COMPARISON` are mutually exclusive; only one verification method should be enabled at a time.
3. According to the current code, double write only support IoTDB v1.0 and newer versions and timescaledb.

## 11. General test mode: write (dual database)

In order to perform the correctness verification below, you first need to write the data to two databases.

## 12. Correctness single point query mode (dual database comparison)

In order to more efficiently verify the correctness of database data, IoT Benchmark provides correctness verification by comparing the data between two databases. Note: Before performing this test, please use the general test mode of writing (dual databases) to complete the database writing. Currently, it is recommended to use the JDBC method.

## 13. Correctness function query mode (dual database comparison)

In order to more efficiently verify the correctness of database queries, IoT Benchmark provides correctness verification by comparing the differences in data query results between two databases.

Note:

1. Before performing this test, please use the general test mode of writing (dual databases) to complete the database writing.

2. The value of LOOP cannot be too large, satisfying: LOOP(query) * QUERY_INTERVAL(query) * DEVICE_NUMBER(write) <= LOOP(write) * POINT_STEP(write)

## 14. Further explanation of correctness verification

1. For dataset-based single-database verification, `verificationQueryMode` is the dedicated mode.
2. For dual-database verification workflows, please refer to the verification guide in [Quick Guide](../verification/README.md).
