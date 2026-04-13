Benchmark SQLite
---

## 1. Environment

Before running the benchmark, prepare:

1. Java 8
2. Maven 3.6+
3. A writable local directory for the benchmark output files

SQLite-specific notes:

1. Due to the characteristics of SQLite, the benchmark can run directly without deploying a separate database service.
2. The benchmark stores data in local database files under the directory where the benchmark is run. In the original SQLite workflow, the generated files include `${DB_NAME}.db`, and the README also refers to `identifier.sqlite`.
3. Due to SQLite write concurrency limitations in this module, only one data client can write to the database file at the same time, so `DATA_CLIENT_NUMBER` must be **1**. If you use the current shared config template, keeping `SCHEMA_CLIENT_NUMBER=1` is also recommended.

## 2. Database setup

SQLite does not require a separate server-side installation or startup step for this benchmark module.

Before running the test:

1. Set `DB_NAME` to the database name you want to use. In the current module, this determines the main local database file name such as `test.db`.
2. Keep `DATA_CLIENT_NUMBER=1`. If you use the current shared config template, also keep `SCHEMA_CLIENT_NUMBER=1`.
3. Make sure the benchmark process has permission to create and update files in the working directory.

Additional notes for this module:

- `CREATE_SCHEMA=true` creates the benchmark tables in the local SQLite database file.
- `IS_DELETE_DATA=true` clears the benchmark tables in the local SQLite database file before the test starts.

## 3. Build benchmark

Build only the SQLite module and its dependencies:

```bash
mvn -pl sqlite -am package -DskipTests
```

This command has been verified locally in this repository.

After packaging, the benchmark tool is generated under:

```text
sqlite/target/iot-benchmark-sqlite
sqlite/target/iot-benchmark-sqlite.zip
```

## 4. Configure benchmark

[Demo config](config.properties)

For the current `sqlite` module, check at least the following items:

| Key             | Required | Description                                                            |
| :-------------- | :------- | :--------------------------------------------------------------------- |
| `DB_SWITCH`            | Yes      | Must be `SQLite`.                                                      |
| `DB_NAME`              | Yes      | Determines the local SQLite database file name, for example `test.db`. |
| `DATA_CLIENT_NUMBER`   | Yes      | Must be `1` for this module.                                           |
| `SCHEMA_CLIENT_NUMBER` | No       | Recommended to keep `1` when using the current shared config template. |
| `DEVICE_NUMBER`        | Yes      | The sample config in this directory uses `1`.                          |
| `SENSOR_NUMBER`        | Yes      | The sample config in this directory uses `6`.                          |

Notes for SQLite configuration:

- This module keeps the common benchmark configuration items such as `HOST`, `PORT`, `USERNAME`, and `PASSWORD`, but the actual benchmark data is written to the local SQLite file determined by `DB_NAME`.
- Other workload parameters such as `LOOP`, `BATCH_SIZE_PER_WRITE`, `OPERATION_PROPORTION`, and `QUERY_INTERVAL` are inherited from the global benchmark configuration template under `configuration/conf/config.properties`.
- The current module supports the common mixed benchmark flow shown in the sample result, including ingestion, precise query, time range query, value range query, aggregation query, `GROUP_BY`, `LATEST_POINT`, `RANGE_QUERY_DESC`, and `VALUE_RANGE_QUERY_DESC`.

The current `sqlite` module does **not** support the following benchmark features:

- `verificationQueryMode`
- comparison or verification paths that require framework verification support, including `IS_COMPARISON=true` and `IS_POINT_COMPARISON=true`
- `GROUP_BY_DESC` in `OPERATION_PROPORTION`
- `SET_OPERATION` in `OPERATION_PROPORTION`
- `ALIGN_BY_DEVICE=true`
- `RESULT_ROW_LIMIT >= 0`

## 5. Run benchmark

Run the benchmark with the packaged scripts:

```bash
cd sqlite/target/iot-benchmark-sqlite
./benchmark.sh
```

If you want to run with a custom configuration directory or file path:

```bash
./benchmark.sh -cf conf
```

For normal read/write benchmark runs, use `BENCHMARK_WORK_MODE=testWithDefaultPath` and keep the unsupported options above disabled.

## 6. Test result

```text
----------------------Main Configurations----------------------
DB_SWITCH: SQLite
OPERATION_PROPORTION: 1:1:1:1:1:1:1:1:1:1:1:0:0
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:1
IS_CLIENT_BIND: true
SCHEMA_CLIENT_NUMBER: 1
DATA_CLIENT_NUMBER: 1
GROUP_NUMBER: 20
DEVICE_NUMBER: 1
SENSOR_NUMBER: 6
BATCH_SIZE_PER_WRITE: 10
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OUT_OF_ORDER: false
OUT_OF_ORDER_MODE: POISSON
OUT_OF_ORDER_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.07 second
Test elapsed time (not include schema creation): 34.76 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           100                 6000                0                   0                   172.62              
PRECISE_POINT       86                  0                   0                   0                   0.00                
TIME_RANGE          87                  4183                0                   0                   120.35              
VALUE_RANGE         95                  4486                0                   0                   129.06              
AGG_RANGE           98                  294                 0                   0                   8.46                
AGG_VALUE           88                  176                 0                   0                   5.06                
AGG_RANGE_VALUE     89                  178                 0                   0                   5.12                
GROUP_BY            92                  1140                0                   0                   32.80               
LATEST_POINT        88                  87                  0                   0                   2.50                
RANGE_QUERY_DESC    87                  3920                0                   0                   112.78              
VALUE_RANGE_QUERY_DESC90                  3948                0                   0                   113.58              
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           338.99      283.08      297.15      324.15      337.24      352.62      357.66      360.86      377.58      629.06      615.16      33898.57    
PRECISE_POINT       0.54        0.29        0.32        0.40        0.50        0.64        0.79        0.82        1.88        1.52        1.48        46.27       
TIME_RANGE          0.64        0.29        0.35        0.46        0.59        0.78        0.93        1.02        1.65        1.45        1.43        55.74       
VALUE_RANGE         0.54        0.29        0.34        0.40        0.51        0.65        0.72        0.90        1.26        1.15        1.14        50.89       
AGG_RANGE           0.53        0.22        0.33        0.42        0.51        0.61        0.72        0.89        1.12        1.08        1.08        51.87       
AGG_VALUE           0.72        0.29        0.46        0.58        0.68        0.79        1.03        1.12        1.71        1.48        1.46        63.22       
AGG_RANGE_VALUE     0.42        0.20        0.25        0.30        0.40        0.52        0.63        0.66        0.95        0.87        0.86        37.80       
GROUP_BY            0.53        0.25        0.33        0.42        0.51        0.63        0.70        0.77        1.07        1.01        1.01        48.55       
LATEST_POINT        0.87        0.40        0.57        0.68        0.79        0.98        1.22        1.29        4.00        2.93        2.81        76.34       
RANGE_QUERY_DESC    0.65        0.30        0.40        0.53        0.63        0.76        0.90        0.98        1.89        1.55        1.51        56.77       
VALUE_RANGE_QUERY_DESC0.52        0.23        0.31        0.38        0.50        0.64        0.72        0.81        0.94        0.92        0.92        46.56       
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```
