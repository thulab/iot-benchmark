Benchmark for timescaleDB
---

## 1. Environment

Before running the benchmark, prepare:

1. A running TimescaleDB instance
2. Java 8
3. Maven 3.6+

## 2. Database setup
1. Run TimescaleDB:

```bash
docker run -it --rm -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=postgres timescale/timescaledb:latest-pg12
```

The default username and password are:

- `USERNAME=postgres`
- `PASSWORD=postgres`

2. Access TimescaleDB:

```bash
docker exec -it timescaledb psql -U postgres
```

More details: <https://hub.docker.com/r/timescale/timescaledb/>

Before running the benchmark:

1. Make sure the PostgreSQL database specified by `DB_NAME` already exists. The sample configuration in this directory uses `DB_NAME=postgres`.
2. Make sure the target database has the TimescaleDB extension available so that the module can create a hypertable.
3. Make sure the benchmark user has permission to create tables and query data in that database.

Additional notes for this module:

- `CREATE_SCHEMA=true` creates a benchmark table in the existing PostgreSQL database and converts it to a TimescaleDB hypertable.
- `CREATE_SCHEMA=true` does not create the PostgreSQL database itself.
- `IS_DELETE_DATA=true` drops the benchmark table created by this module in the connected database.
- In the current implementation, `DB_NAME` is used both as the JDBC database name and as the benchmark table name created inside that database.

## 3. Build benchmark

Build only the TimescaleDB module and its dependencies:

```bash
mvn -pl timescaledb -am package -DskipTests
```

This command has been verified locally in this repository.

After packaging, the benchmark tool is generated under:

```text
timescaledb/target/iot-benchmark-timescaledb
timescaledb/target/iot-benchmark-timescaledb.zip
```

## 4. Configure benchmark

[Demo config](config.properties)

For the current `timescaledb` module, check at least the following items:

| Key         | Required | Description                                                                                    |
| :---------- | :------- | :--------------------------------------------------------------------------------------------- |
| `DB_SWITCH` | Yes      | Must be `TimescaleDB`.                                                                         |
| `HOST`      | Yes      | Target TimescaleDB or PostgreSQL host. The sample uses `127.0.0.1`.                            |
| `PORT`      | Yes      | Target PostgreSQL port. The sample uses `5432`.                                                |
| `USERNAME`  | Yes      | Database username. The Docker example uses `postgres`.                                         |
| `PASSWORD`  | Yes      | Database password. The Docker example uses `postgres`.                                         |
| `DB_NAME`   | Yes      | Existing PostgreSQL database name. In this module it is also used as the benchmark table name. |

Minimal example:

```properties
DB_SWITCH=TimescaleDB
HOST=127.0.0.1
PORT=5432
USERNAME=postgres
PASSWORD=postgres
DB_NAME=postgres
```

Notes for TimescaleDB configuration:

- `DB_NAME` should be an existing database that the benchmark can connect to.
- This module also creates and drops a benchmark table with the same name as `DB_NAME`, so avoid using a database name that conflicts with your own table naming policy.
- Other workload parameters such as `CLIENT_NUMBER`, `LOOP`, `BATCH_SIZE_PER_WRITE`, `OPERATION_PROPORTION`, and `QUERY_INTERVAL` are inherited from the global benchmark configuration template under `configuration/conf/config.properties`.
- This module supports `verificationQueryMode`, comparison paths, and `SET_OPERATION`.

The current `timescaledb` module does **not** support the following benchmark features:

- `GROUP_BY_DESC` in `OPERATION_PROPORTION`
- `ALIGN_BY_DEVICE=true`
- `RESULT_ROW_LIMIT >= 0`

## 5. Run benchmark

Run the benchmark with the packaged scripts:

```bash
cd timescaledb/target/iot-benchmark-timescaledb
./benchmark.sh
```

If you want to run with a custom configuration directory or file path:

```bash
./benchmark.sh -cf conf
```

For a normal mixed read/write benchmark, use `BENCHMARK_WORK_MODE=testWithDefaultPath` and keep the unsupported options above disabled.

## 6. Test result

```text
----------------------Main Configurations----------------------
DB_SWITCH: TimescaleDB
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
Create schema cost 0.04 second
Test elapsed time (not include schema creation): 6.75 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           478                 47800               0                   0                   7078.38             
PRECISE_POINT       462                 412                 0                   0                   61.01               
TIME_RANGE          438                 15334               0                   0                   2270.71             
VALUE_RANGE         483                 236990              0                   0                   35094.23            
AGG_RANGE           425                 379                 0                   0                   56.12               
AGG_VALUE           460                 459                 0                   0                   67.97               
AGG_RANGE_VALUE     431                 381                 0                   0                   56.42               
GROUP_BY            473                 17143               0                   0                   2538.59             
LATEST_POINT        459                 454                 0                   0                   67.23               
RANGE_QUERY_DESC    433                 15354               0                   0                   2273.67             
VALUE_RANGE_QUERY_DESC458                 224520              0                   0                   33247.63            
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           20.03       3.47        10.97       14.32       17.68       23.01       31.95       40.28       56.86       83.51       81.28       1980.82     
PRECISE_POINT       3.82        0.50        1.17        1.93        3.49        5.24        6.90        7.90        10.59       16.01       15.16       379.33      
TIME_RANGE          4.22        0.70        1.30        2.17        3.90        5.65        7.66        8.84        11.59       15.88       15.19       409.78      
VALUE_RANGE         6.53        1.21        2.09        3.24        5.35        8.98        12.33       14.46       18.80       50.92       41.08       684.43      
AGG_RANGE           4.04        0.58        1.23        1.96        3.41        5.45        7.50        9.05        10.83       48.12       37.96       394.35      
AGG_VALUE           4.75        0.90        1.48        2.45        4.30        6.29        8.56        10.07       12.82       23.54       20.41       489.52      
AGG_RANGE_VALUE     4.45        0.67        1.39        2.25        3.91        5.93        7.51        9.86        13.79       21.21       19.79       426.36      
GROUP_BY            4.69        0.75        1.45        2.40        4.14        6.16        8.38        10.91       14.20       22.41       20.51       495.48      
LATEST_POINT        4.43        0.45        1.21        1.88        3.49        5.08        7.59        9.32        36.00       89.18       78.91       514.62      
RANGE_QUERY_DESC    4.44        0.64        1.38        2.35        4.00        5.91        8.02        9.11        13.21       22.97       21.09       393.13      
VALUE_RANGE_QUERY_DESC8.45        1.11        2.57        4.73        7.80        11.30       14.80       16.87       22.05       24.87       24.47       843.69      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```
