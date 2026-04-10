# Benchmark for KairosDB

This module uses IoT Benchmark to test KairosDB.

## 1. Environment

Before running the benchmark, prepare:

1. Java 8
2. Maven 3.6+
3. A running KairosDB instance reachable from the benchmark machine

Install and start KairosDB with commands similar to the following:

1. Download and decompress the installation file:

```shell
sudo tar -zxvf kairosdb-version.tar.gz -C /path/to/kairosdb
```

2. Start KairosDB in background:

```shell
cd /path/to/kairosdb
./bin/kairosdb.sh start
```

Recommended KairosDB checks:

- Ensure the HTTP API is enabled and reachable. The sample configuration in this directory uses port `8080`.
- Ensure the write API `/api/v1/datapoints` and query API are reachable from the benchmark machine.
- If you use value-filter queries, plan to set `QUERY_LOWER_VALUE >= 0`.

## 2. Database setup

KairosDB does not require a separate benchmark database or schema creation step before running this module.

Notes specific to KairosDB:

- The benchmark writes sensor data as KairosDB metrics and uses tags such as group and device for query filtering.
- `CREATE_SCHEMA=true` does not create extra schema objects for this module.
- `DB_NAME` is present in the shared benchmark configuration model, but the current `kairosdb` module does not use it to isolate benchmark data.
- If `IS_DELETE_DATA=true`, the cleanup step deletes all non-internal metrics visible to the current KairosDB instance. Do not enable it against a shared or production environment.

## 3. Build benchmark

Build only the KairosDB module and its dependencies:

```bash
mvn -pl kairosdb -am package -DskipTests
```

This command has been verified locally in this repository.

After packaging, the benchmark tool is generated under:

```text
kairosdb/target/iot-benchmark-kairosdb
kairosdb/target/iot-benchmark-kairosdb.zip
```

## 4. Configure benchmark

There is a [sample configuration file](./config.properties).

For the current `kairosdb` module, check at least the following items:

| Key | Required | Description |
| :-- | :-- | :-- |
| `DB_SWITCH` | Yes | Must be `KairosDB`. |
| `HOST` | Yes | Target KairosDB host. If multiple hosts are configured in the framework, this module currently uses only the first one. |
| `PORT` | Yes | Target HTTP API port. The sample uses `8080`. |
| `QUERY_LOWER_VALUE` | Yes for value-filter workloads | Because of the rule in KairosDB, `QUERY_LOWER_VALUE` must be greater than or equal to `0`. |
| `DB_NAME` | No | Present in the shared configuration model, but not used by the current `kairosdb` module. |
| `USERNAME` | No | Present in the shared configuration model, but not used by the current `kairosdb` module. |
| `PASSWORD` | No | Present in the shared configuration model, but not used by the current `kairosdb` module. |

When value-filter queries are enabled, modify the following parameter in `conf/config.properties`:

```properties
QUERY_LOWER_VALUE=0
```

Minimal example:

```properties
DB_SWITCH=KairosDB
HOST=127.0.0.1
PORT=8080
DB_NAME=test
USERNAME=root
PASSWORD=root
QUERY_LOWER_VALUE=0
```

Other workload parameters such as `CLIENT_NUMBER`, `LOOP`, `BATCH_SIZE_PER_WRITE`, `OPERATION_PROPORTION`, and `QUERY_INTERVAL` are inherited from the global benchmark configuration template under `configuration/conf/config.properties`.

The current `kairosdb` module does **not** support the following benchmark features:

- `verificationQueryMode`
- comparison or verification paths that require framework verification support, including `IS_COMPARISON=true` and `IS_POINT_COMPARISON=true`
- `GROUP_BY_DESC` in `OPERATION_PROPORTION`
- `SET_OPERATION` in `OPERATION_PROPORTION`
- `ALIGN_BY_DEVICE=true`
- `RESULT_ROW_LIMIT >= 0`

## 5. Run benchmark

Run the benchmark with the packaged scripts:

```bash
cd kairosdb/target/iot-benchmark-kairosdb
./benchmark.sh
```

If you want to run with a custom configuration directory or file path:

```bash
./benchmark.sh -cf conf
```

For a normal mixed read/write benchmark, use `BENCHMARK_WORK_MODE=testWithDefaultPath` and avoid unsupported operations listed above.

The test result will be printed in the console and recorded in the generated `logs` directory during execution.

## 6. Test result

```text
----------------------Main Configurations----------------------
BENCHMARK_WORK_MODE=testWithDefaultPath
RESULT_PRECISION=0.1%
DBConfig=
  DB_SWITCH=KairosDB
  HOST=[192.168.174.101]
  PORT=[8080]
  USERNAME=root
  PASSWORD=root
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=false
BENCHMARK_CLUSTER=false
OPERATION_PROPORTION=0:1:1:1:1:1:1:1:1:1:1
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
IS_DELETE_DATA=false
CREATE_SCHEMA=false
IS_CLIENT_BIND=true
CLIENT_NUMBER=5
GROUP_NUMBER=20
SG_STRATEGY=mod
DEVICE_NUMBER=5
REAL_INSERT_RATE=1.0
SENSOR_NUMBER=10
IS_SENSOR_TS_ALIGNMENT=true
BATCH_SIZE_PER_WRITE=10
LOOP=1000
POINT_STEP=5000
OP_MIN_INTERVAL=0
QUERY_INTERVAL=250000
IS_OUT_OF_ORDER=false
OUT_OF_ORDER_MODE=0
OUT_OF_ORDER_RATIO=0.5
IS_REGULAR_FREQUENCY=true
START_TIME=2018-9-20T00:00:00+08:00
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 9.64 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           0                   0                   0                   0                   0.00                
PRECISE_POINT       532                 532                 0                   0                   55.21               
TIME_RANGE          489                 24939               0                   0                   2587.94             
VALUE_RANGE         29                  102                 0                   0                   10.58               
AGG_RANGE           519                 519                 0                   0                   53.86               
AGG_VALUE           29                  1                   0                   0                   0.10                
AGG_RANGE_VALUE     32                  1                   0                   0                   0.10                
GROUP_BY            491                 6383                0                   0                   662.37              
LATEST_POINT        514                 514                 0                   0                   53.34               
RANGE_QUERY_DESC    482                 24582               0                   0                   2550.89             
VALUE_RANGE_QUERY_DESC39                  459                 0                   0                   47.63               
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT       10.69       3.19        4.12        5.92        8.79        12.45       19.02       24.33       36.62       170.15      126.51      1250.41     
TIME_RANGE          12.09       3.52        4.53        6.54        10.04       15.17       21.43       27.08       46.99       79.90       76.27       1300.06     
VALUE_RANGE         19.89       5.18        5.24        11.12       14.62       26.10       38.86       39.19       49.63       47.58       47.35       147.16      
AGG_RANGE           11.25       3.35        4.52        6.24        9.12        13.14       20.51       24.85       38.51       83.24       76.69       1393.22     
AGG_VALUE           258.73      97.18       98.95       168.64      219.92      253.94      492.78      553.35      1027.55     944.99      935.82      1697.52     
AGG_RANGE_VALUE     19.64       9.00        9.76        11.32       15.45       22.71       31.40       36.89       80.33       71.23       70.22       176.03      
GROUP_BY            10.94       3.33        4.39        5.81        8.57        12.68       19.36       24.92       51.28       131.41      112.85      1242.27     
LATEST_POINT        14.49       3.53        4.50        6.47        9.31        12.69       20.74       28.42       38.88       380.66      380.64      1722.99     
RANGE_QUERY_DESC    11.11       3.35        4.60        6.31        9.17        12.72       19.19       24.64       41.69       78.83       76.07       1218.01     
VALUE_RANGE_QUERY_DESC22.88       8.62        9.28        12.74       17.39       25.59       37.71       57.09       158.96      133.41      130.57      235.26      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```
