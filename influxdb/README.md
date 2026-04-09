# Benchmark for InfluxDB 1.x

This module uses `iot-benchmark` to test InfluxDB 1.x.

## 1. Environment

Prepare the following environment before running the benchmark:

1. Java 8
2. Maven 3.6+
3. A running InfluxDB 1.x instance reachable from the benchmark machine

The original benchmark result in this directory was produced with InfluxDB `1.8.6-1`

This module is intended for InfluxDB 1.x and therefore uses:

- an InfluxDB database named by `DB_NAME`, instead of bucket and organization
- InfluxQL for the benchmark query workload
- no token-based authentication configuration

Recommended checks:

- Ensure the HTTP API is enabled and reachable. The sample configuration in this directory uses port `8086`.
- Ensure the benchmark process is allowed to create and drop databases if `CREATE_SCHEMA=true` or `IS_DELETE_DATA=true`.
- If your InfluxDB deployment requires authentication, this module cannot directly use `USERNAME` and `PASSWORD` for the benchmark connection.

## 2. Database setup

Before running the benchmark, prepare a target InfluxDB 1.x instance and a benchmark database.

If `CREATE_SCHEMA=true`, the benchmark will create the database named by `DB_NAME` before writing. If you prefer to prepare it manually, create the database in advance, for example:

```sql
CREATE DATABASE test
```

Notes specific to InfluxDB 1.x:

- `DB_NAME` maps to an InfluxDB **database**.
- Benchmark data is written to the default retention policy `autogen`.
- If `IS_DELETE_DATA=true`, the benchmark will drop the whole target database during cleanup. Do not point `DB_NAME` to a database that contains production data.
- The current module only uses the first configured `HOST` and `PORT`.

## 3. Build benchmark

Build only the InfluxDB 1.x module and its dependencies:

```bash
mvn -pl influxdb -am package -DskipTests
```

This command has been verified locally in this repository.

After packaging, the benchmark tool is generated under:

```text
influxdb/target/iot-benchmark-influxdb
influxdb/target/iot-benchmark-influxdb.zip
```

## 4. Configure benchmark

There is a sample configuration file at [config.properties](./config.properties).

For the current `influxdb` module, check at least the following items:

| Key                   | Required | Description                                                                                                             |
| :-------------------- | :------- | :---------------------------------------------------------------------------------------------------------------------- |
| `DB_SWITCH`           | Yes      | Must be `InfluxDB`.                                                                                                     |
| `HOST`                | Yes      | Target InfluxDB host. If multiple hosts are configured in the framework, this module currently uses only the first one. |
| `PORT`                | Yes      | Target HTTP API port. The sample uses `8086`.                                                                           |
| `DB_NAME`             | Yes      | Target database name in InfluxDB 1.x.                                                                                   |
| `USERNAME`            | No       | Present in the shared configuration model, but not used by the current `influxdb` module connection logic.              |
| `PASSWORD`            | No       | Present in the shared configuration model, but not used by the current `influxdb` module connection logic.              |
| `TIMESTAMP_PRECISION` | No       | `ms`, `us`, and `ns` are supported by this module. Default is `ms`.                                                     |
| `RESULT_ROW_LIMIT`    | No       | Supported by this module. When configured, it is appended to query SQL as `limit N`.                                    |
| `ALIGN_BY_DEVICE`     | No       | Supported by this module.                                                                                               |

Minimal example:

```properties
DB_SWITCH=InfluxDB
HOST=127.0.0.1
PORT=8086
DB_NAME=test
USERNAME=root
PASSWORD=root
TIMESTAMP_PRECISION=ms
```

Other workload parameters such as `CLIENT_NUMBER`, `LOOP`, `BATCH_SIZE_PER_WRITE`, `OPERATION_PROPORTION`, and `QUERY_INTERVAL` are inherited from the global template under `configuration/conf/config.properties`.

The current `influxdb` module does **not** support the following benchmark features:

- `IS_DOUBLE_WRITE=true` when InfluxDB 1.x participates in `testWithDefaultPath`
- point-by-point comparison based on device-level verification interfaces, including `IS_POINT_COMPARISON=true`
- `SET_OPERATION` in `OPERATION_PROPORTION`
- authenticated InfluxDB 1.x deployments that require the benchmark to log in with `USERNAME` and `PASSWORD`

Supported but easy to confuse with the InfluxDB 2.x module:

- `verificationQueryMode` is supported for InfluxDB 1.x
- `GROUP_BY_DESC` is supported
- `RESULT_ROW_LIMIT` is supported
- `ALIGN_BY_DEVICE` is supported

## 5. Run benchmark

Run the benchmark with the packaged scripts:

```bash
cd influxdb/target/iot-benchmark-influxdb
./benchmark.sh
```

If you want to use a custom configuration directory or file path:

```bash
./benchmark.sh -cf conf
```

For a normal mixed read/write benchmark, use `BENCHMARK_WORK_MODE=testWithDefaultPath`.

If you want to run correctness verification queries against a single InfluxDB 1.x instance, `verificationQueryMode` is available for this module. Avoid enabling unsupported options listed above at the same time.

The test result will be printed in the console and recorded in the generated `logs` directory during execution.

## 6. Test result

The following sample result comes from the original README content in this directory:

```text
----------------------Main Configurations----------------------
DB_SWITCH: InfluxDB
OPERATION_PROPORTION: 1:1:1:1:1:1:1:1:1:1:1
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:1
IS_CLIENT_BIND: true
CLIENT_NUMBER: 20
GROUP_NUMBER: 20
DEVICE_NUMBER: 20
SENSOR_NUMBER: 300
BATCH_SIZE_PER_WRITE: 10
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OUT_OF_ORDER: false
OUT_OF_ORDER_MODE: 0
OUT_OF_ORDER_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.02 second
Test elapsed time (not include schema creation): 23.81 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s)
INGESTION           1824                5472000             0                   0                   229823.86
PRECISE_POINT       1831                0                   0                   0                   0.00
TIME_RANGE          1776                58026               0                   0                   2437.09
VALUE_RANGE         1871                61099               0                   0                   2566.16
AGG_RANGE           1734                1403                0                   0                   58.93
AGG_VALUE           1769                1757                0                   0                   73.79
AGG_RANGE_VALUE     1790                1478                0                   0                   62.08
GROUP_BY            1912                21344               0                   0                   896.45
LATEST_POINT        1841                1809                0                   0                   75.98
RANGE_QUERY_DESC    1875                59689               0                   0                   2506.94
VALUE_RANGE_QUERY_DESC1777                57475               0                   0                   2413.95
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           49.53       19.05       32.12       35.50       40.55       49.93       66.68       86.37       171.74      635.29      652.59      5115.04
PRECISE_POINT       19.85       1.07        8.90        11.23       15.71       23.49       34.36       44.64       83.47       165.52      218.22      2427.25
TIME_RANGE          20.41       1.74        9.73        11.85       15.97       23.60       34.78       45.81       93.63       138.73      179.34      2054.51
VALUE_RANGE         21.50       1.84        9.76        12.26       16.32       24.95       36.28       49.40       102.84      209.85      297.07      2474.57
AGG_RANGE           20.27       1.79        9.50        11.69       15.60       23.28       33.22       44.46       102.41      205.31      257.87      2408.83
AGG_VALUE           21.15       2.50        10.14       12.32       16.24       23.97       36.39       45.56       84.75       278.99      347.58      2354.46
AGG_RANGE_VALUE     20.80       1.59        9.47        11.75       16.12       23.72       34.43       46.94       100.46      181.82      270.81      2516.03
GROUP_BY            20.54       1.39        9.82        11.80       15.84       23.26       35.74       47.68       88.66       141.62      170.65      2425.34
LATEST_POINT        20.56       1.43        9.51        11.45       15.79       23.66       35.70       45.27       83.91       230.50      246.98      2528.49
RANGE_QUERY_DESC    21.35       1.88        9.76        12.10       16.42       24.42       36.04       47.86       105.55      205.40      221.11      2627.79
VALUE_RANGE_QUERY_DESC21.97       1.49        9.88        12.22       16.45       24.63       35.99       52.92       111.61      231.46      291.88      2506.34
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```
