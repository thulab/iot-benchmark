Benchmark Questdb
---

1. Please note: the `CLIENT_NUMBER` of QuestDB needs to be configured together with the server startup settings, and it should be less than or equal to `pg.net.active.connection.limit` and the available worker capacity.
2. Please note: the number of sensors should not be too large for this module.

## 1. Environment

Before running the benchmark, prepare:

1. A running QuestDB instance
2. Java 8
3. Maven 3.6+

QuestDB-specific notes:

- This module connects through the PostgreSQL wire protocol. The sample configuration in this directory uses port `8812`.
- The benchmark client concurrency should be planned together with the QuestDB server settings.
- The sample QuestDB deployment in the original README uses Docker.

Example environment preparation with Docker:

1. Pull the image:

```bash
docker pull questdb/questdb
```

2. Run the image:

```bash
docker run --rm -p 9000:9000 -p 9009:9009 -p 8812:8812 -p 9003:9003 -e QDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL=1 -e QDB_PG_NET_ACTIVE_CONNECTION_LIMIT=20 --name=questdb questdb/questdb
```

## 2. Database setup

For server deployment, execute the following settings before starting QuestDB. For more reference: <https://questdb.io/docs/reference/configuration#postgres-wire-protocol>

```bash
export QDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL=1
export QDB_PG_NET_ACTIVE_CONNECTION_LIMIT=20
```

If you use the configuration file `conf/server.conf`, the corresponding file is usually located at `/usr/local/var/questdb/conf/server.conf` or `$HOME/.questdb/conf/server.conf`. The original README recommends the following parameter changes:

```conf
line.tcp.maintenance.job.interval=1
pg.net.active.connection.limit=20
```

Additional notes for this module:

- `CLIENT_NUMBER` should not exceed the configured QuestDB connection limit.
- The current module creates benchmark tables automatically when `CREATE_SCHEMA=true`.
- `IS_DELETE_DATA=true` drops the benchmark tables whose names start with `DB_NAME`.
- QuestDB uses the built-in `qdb` database over the PostgreSQL wire protocol. `DB_NAME` in this module is used as the benchmark table name prefix, not as a separate database name to create first.

## 3. Build benchmark

Build only the QuestDB module and its dependencies:

```bash
mvn -pl questdb -am package -DskipTests
```

This command has been verified locally in this repository.

After packaging, the benchmark tool is generated under:

```text
questdb/target/iot-benchmark-questdb
questdb/target/iot-benchmark-questdb.zip
```

## 4. Configure benchmark

There is a [Demo config](config.properties).

For the current `questdb` module, check at least the following items:

| Key         | Required | Description                                                                                                            |
| :---------- | :------- | :--------------------------------------------------------------------------------------------------------------------- |
| `DB_SWITCH` | Yes      | Must be `QuestDB`.                                                                                                     |
| `HOST`      | Yes      | Target QuestDB host. If multiple hosts are configured in the framework, this module currently uses only the first one. |
| `PORT`      | Yes      | Target PostgreSQL wire protocol port. The sample uses `8812`.                                                          |
| `USERNAME`  | Yes      | QuestDB username used by the PostgreSQL wire connection.                                                               |
| `PASSWORD`  | Yes      | Password for the QuestDB user.                                                                                         |
| `DB_NAME`   | Yes      | Prefix used by this module when creating benchmark tables.                                                             |

Minimal example:

```properties
DB_SWITCH=QuestDB
HOST=127.0.0.1
PORT=8812
USERNAME=admin
PASSWORD=quest
DB_NAME=test
```

Other workload parameters such as `CLIENT_NUMBER`, `LOOP`, `BATCH_SIZE_PER_WRITE`, `OPERATION_PROPORTION`, and `QUERY_INTERVAL` are inherited from the global benchmark configuration template under `configuration/conf/config.properties`.

The current `questdb` module does **not** support the following benchmark features:

- `verificationQueryMode`
- comparison or verification paths that require framework verification support, including `IS_COMPARISON=true` and `IS_POINT_COMPARISON=true`
- `GROUP_BY_DESC` in `OPERATION_PROPORTION`
- `SET_OPERATION` in `OPERATION_PROPORTION`
- `ALIGN_BY_DEVICE=true`
- `RESULT_ROW_LIMIT >= 0`

## 5. Run benchmark

Run the benchmark with the packaged scripts:

```bash
cd questdb/target/iot-benchmark-questdb
./benchmark.sh
```

If you want to run with a custom configuration directory or file path:

```bash
./benchmark.sh -cf conf
```

For a normal mixed read/write benchmark, use `BENCHMARK_WORK_MODE=testWithDefaultPath` and keep unsupported options disabled.

## 6. Test result
```
----------------------Main Configurations----------------------
DB_SWITCH: QuestDB
OPERATION_PROPORTION: 1:1:1:1:1:1:1:1:1:1:1
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:1
IS_CLIENT_BIND: true
CLIENT_NUMBER: 5
GROUP_NUMBER: 20
DEVICE_NUMBER: 5
SENSOR_NUMBER: 10
BATCH_SIZE_PER_WRITE: 10
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OUT_OF_ORDER: false
OUT_OF_ORDER_MODE: 0
OUT_OF_ORDER_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.27 second
Test elapsed time (not include schema creation): 58.72 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           478                 47800               0                   0                   814.07              
PRECISE_POINT       462                 280                 0                   0                   4.77                
TIME_RANGE          438                 60215               0                   0                   1025.50             
VALUE_RANGE         483                 62985               0                   0                   1072.68             
AGG_RANGE           425                 2125                0                   0                   36.19               
AGG_VALUE           460                 2300                0                   0                   39.17               
AGG_RANGE_VALUE     431                 2155                0                   0                   36.70               
GROUP_BY            473                 2365                0                   0                   40.28               
LATEST_POINT        459                 12475               0                   0                   212.46              
RANGE_QUERY_DESC    433                 60065               0                   0                   1022.95             
VALUE_RANGE_QUERY_DESC458                 64295               0                   0                   1094.99             
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           38.78       11.42       18.21       23.36       29.80       41.68       58.49       81.91       175.96      781.66      660.40      5817.19     
PRECISE_POINT       2.95        0.23        0.46        0.90        2.06        3.71        5.75        8.02        15.23       35.30       35.26       550.05      
TIME_RANGE          2.75        0.24        0.44        0.90        2.21        3.66        5.98        7.52        10.80       33.11       27.14       330.93      
VALUE_RANGE         3.43        0.30        0.52        1.18        2.49        4.19        6.78        8.45        13.30       161.70      115.03      575.68      
AGG_RANGE           2.51        0.24        0.44        0.73        1.90        3.36        5.45        6.77        10.24       23.25       21.35       419.40      
AGG_VALUE           2.60        0.20        0.34        0.84        1.89        3.36        5.39        7.40        12.13       19.34       17.66       449.66      
AGG_RANGE_VALUE     2.80        0.30        0.51        0.94        2.07        3.49        6.03        8.39        13.11       19.38       18.34       470.92      
GROUP_BY            2.92        0.25        0.46        0.84        2.04        3.63        5.79        7.63        20.98       34.86       34.16       527.69      
LATEST_POINT        382.48      5.49        8.10        12.73       37.64       57.96       113.85      177.18      1107.07     53875.53    50683.53    53338.54    
RANGE_QUERY_DESC    5.00        0.50        2.19        3.02        4.29        5.78        8.32        11.68       18.99       27.52       26.22       656.54      
VALUE_RANGE_QUERY_DESC5.78        0.54        2.12        2.95        4.24        6.23        9.43        11.69       25.13       280.81      210.70      995.24      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

Addition: When deploy to server, you may meet connection attempted failed bug, relates: https://github.com/pgjdbc/pgjdbc/issues/1871
