# Benchmark for CnosDB

This module uses IoT Benchmark to test CnosDB.

## 1. Environment

Before running the benchmark, prepare:

1. Java 8
2. Maven 3.6+
3. A running CnosDB instance reachable from the benchmark machine

This module has the following characteristics:

- It uses the CnosDB HTTP SQL service. The sample configuration in this directory uses port `8902`.
- `DB_NAME` maps to a CnosDB database.
- `CNOSDB_SHARD_NUMBER` is a CnosDB-specific configuration used when creating the benchmark database.
- The current implementation uses `Authorization: Basic cm9vdDo=` by default, which means `root` user with an empty password.

Recommended CnosDB checks:

- Enable and expose the HTTP SQL service port.
- Ensure the benchmark user can create and drop databases.
- If your CnosDB instance does not accept `root` with an empty password, you need to adjust the authorization setting in the current module before running the benchmark.

## 2. Database setup

Before running the benchmark, prepare a target CnosDB instance and a benchmark database.

If `CREATE_SCHEMA=true`, the benchmark will create the database named by `DB_NAME` before writing, using the configured `CNOSDB_SHARD_NUMBER`. If you prefer to prepare it manually, create the database in advance, for example:

```sql
create database if not exists test with shard 32;
```

Notes specific to CnosDB:

- `DB_NAME` maps to a CnosDB **database**.
- If `IS_DELETE_DATA=true`, the benchmark will drop the whole target database during cleanup. Do not point `DB_NAME` to a database that contains production data.
- The current module only uses the first configured `HOST` and `PORT`.
- Group-by queries are translated to CnosDB time bucketing semantics, so benchmark behavior should be validated against your target CnosDB version before comparing results across systems.

## 3. Build benchmark

Build only the CnosDB module and its dependencies:

```bash
mvn -pl cnosdb -am package -DskipTests
```

This command has been verified locally in this repository.

After packaging, the runnable distribution is generated under:

```text
cnosdb/target/iot-benchmark-cnosdb
cnosdb/target/iot-benchmark-cnosdb.zip
```

## 4. Configure benchmark

There is a sample configuration file at [config.properties](./config.properties).

For the current `cnosdb` module, check at least the following items:

| Key                   | Required | Description                                                                                                           |
| :-------------------- | :------- | :-------------------------------------------------------------------------------------------------------------------- |
| `DB_SWITCH`           | Yes      | Must be `CnosDB`.                                                                                                     |
| `HOST`                | Yes      | Target CnosDB host. If multiple hosts are configured in the framework, this module currently uses only the first one. |
| `PORT`                | Yes      | Target HTTP SQL port. The sample uses `8902`.                                                                         |
| `DB_NAME`             | Yes      | Target database name in CnosDB.                                                                                       |
| `CNOSDB_SHARD_NUMBER` | No       | Shard count used when creating the benchmark database. Default is `32`.                                               |
| `USERNAME`            | No       | Not used by the current `cnosdb` module implementation.                                                               |
| `PASSWORD`            | No       | Not used by the current `cnosdb` module implementation.                                                               |

Minimal example:

```properties
DB_SWITCH=CnosDB
HOST=127.0.0.1
PORT=8902
DB_NAME=test
CNOSDB_SHARD_NUMBER=32
```

Other workload parameters such as `SCHEMA_CLIENT_NUMBER`, `DATA_CLIENT_NUMBER`, `LOOP`, `BATCH_SIZE_PER_WRITE`, `OPERATION_PROPORTION`, and query ratios are inherited from the global benchmark configuration template under `configuration/conf/config.properties`.

The current `cnosdb` module does **not** support the following benchmark features:

- `SET_OPERATION` in `OPERATION_PROPORTION`
- point-by-point comparison based on device-level verification interfaces, including `IS_POINT_COMPARISON=true`
- `ALIGN_BY_DEVICE=true`
- `RESULT_ROW_LIMIT >= 0`
- using `USERNAME` and `PASSWORD` in `config.properties` to authenticate the benchmark connection

The current `cnosdb` module supports the following benchmark features:

- `verificationQueryMode` is supported for CnosDB
- `GROUP_BY_DESC` is supported
- record-based verification/comparison through `verificationQuery` is supported when the overall benchmark configuration is valid

## 5. Run benchmark

Run the benchmark with the packaged scripts:

```bash
cd cnosdb/target/iot-benchmark-cnosdb
./benchmark.sh
```

If you want to run with a custom configuration directory or file path:

```bash
./benchmark.sh -cf conf
```

For a normal mixed read/write benchmark, use `BENCHMARK_WORK_MODE=testWithDefaultPath`.

If you want to run correctness verification queries against CnosDB, `verificationQueryMode` is available for this module. Avoid enabling unsupported options listed above at the same time.

The test result will be printed in the console and recorded in the generated `logs` directory during execution.

## 6. Test result

```text
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=false
DBConfig=
  DB_SWITCH=CnosDB
  HOST=[127.0.0.1]
########### Data Mode ###########
GROUP_NUMBER=20
DEVICE_NUMBER=20
REAL_INSERT_RATE=1.0
SENSOR_NUMBER=300
IS_SENSOR_TS_ALIGNMENT=true
IS_OUT_OF_ORDER=false
OUT_OF_ORDER_RATIO=0.5
########### Data Amount ###########
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1:1:0
SCHEMA_CLIENT_NUMBER=20
DATA_CLIENT_NUMBER=20
LOOP=1000
BATCH_SIZE_PER_WRITE=10
DEVICE_NUM_PER_WRITE=1
START_TIME=2022-01-01T00:00:00+08:00
POINT_STEP=5000
OP_MIN_INTERVAL=0
OP_MIN_INTERVAL_RANDOM=false
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
ENCODINGS=RLE/TS_2DIFF/TS_2DIFF/GORILLA/GORILLA/DICTIONARY
COMPRESSOR=LZ4
########### Query Param ###########
QUERY_DEVICE_NUM=1
QUERY_SENSOR_NUM=1
QUERY_INTERVAL=250000
STEP_SIZE=1
IS_RECENT_QUERY=false
########### Other Param ###########
IS_DELETE_DATA=true
CREATE_SCHEMA=true
BENCHMARK_CLUSTER=false
VECTOR=true

---------------------------------------------------------------
main measurements:
Create schema cost 0.03 second
Test elapsed time (not include schema creation): 710.18 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)
INGESTION                1668                     5004000                  0                        0                        7046.06
PRECISE_POINT            1688                     24                       0                        0                        0.03
TIME_RANGE               1609                     155044                   0                        0                        218.32
VALUE_RANGE              1724                     166220                   0                        0                        234.05
AGG_RANGE                1634                     1650                     0                        0                        2.32
AGG_VALUE                1589                     1603                     0                        0                        2.26
AGG_RANGE_VALUE          1618                     1631                     0                        0                        2.30
GROUP_BY                 1654                     21277                    0                        0                        29.96
LATEST_POINT             1790                     1825                     0                        0                        2.57
RANGE_QUERY_DESC         1706                     164454                   0                        0                        231.57
VALUE_RANGE_QUERY_DESC   1686                     162880                   0                        0                        229.35
GROUP_BY_DESC            1634                     20921                    0                        0                        29.46
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION                488.67      25.38       297.81      372.06      472.59      585.16      686.56      780.46      931.55      2365.09     2608.27     48215.75
PRECISE_POINT            703.79      4.50        509.56      608.36      688.93      811.19      915.26      983.28      1144.30     1376.49     1404.58     67672.06
TIME_RANGE               703.09      3.94        496.42      610.65      691.23      809.82      923.24      997.29      1137.73     1305.43     1449.56     64305.71
VALUE_RANGE              710.36      3.61        508.36      613.90      706.27      812.29      924.29      977.06      1114.60     1413.82     1454.81     68056.47
AGG_RANGE                710.81      3.46        506.80      615.11      701.39      816.73      925.90      993.49      1148.46     1300.30     1473.81     75751.07
AGG_VALUE                714.49      4.99        515.91      616.42      702.66      823.51      925.31      996.28      1145.38     1333.55     1415.87     65565.50
AGG_RANGE_VALUE          719.29      2.92        529.65      623.54      709.55      823.32      927.30      997.72      1133.30     1227.32     1261.44     77830.08
GROUP_BY                 788.29      6.62        560.39      653.02      771.46      915.59      1046.47     1153.96     1306.75     1479.48     1652.89     79228.42
LATEST_POINT             709.25      3.17        496.80      615.54      701.74      819.30      944.59      1002.03     1136.95     1239.04     1249.00     75956.11
RANGE_QUERY_DESC         699.29      3.46        496.19      604.62      685.97      805.92      911.87      965.89      1128.06     1283.03     1295.40     69943.04
VALUE_RANGE_QUERY_DESC   709.52      5.48        512.47      617.11      701.39      813.19      917.91      977.22      1141.64     1369.16     1411.72     68471.95
GROUP_BY_DESC            787.70      5.28        560.25      648.31      773.14      921.28      1048.09     1147.17     1265.12     1528.68     1603.18     74526.22
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```
