Benchmark Microsoft SQL Server
---

## 1. Environment

Before running the benchmark, prepare:

1. Microsoft SQL Server `2016 SP2` Standard Edition or a compatible newer version
2. A management client such as Microsoft SQL Server Management Studio (SSMS) or `sqlcmd`
3. Java 8
4. Maven 3.6+

Notes specific to Microsoft SQL Server:

- The sample configuration in this directory uses port `1433`.
- The benchmark connects with the JDBC driver and SQL Server authentication, so username/password login must be enabled.
- The benchmark database itself must already exist before the module starts.

## 2. Database setup

Before running the benchmark, make sure the following conditions are met:

1. Microsoft SQL Server must open port `1433`.
2. Microsoft SQL Server must allow access via `TCP/IP`.
3. In Microsoft SQL Server, a database consistent with `DB_NAME` must have been created in advance. With `sqlcmd`, for example:

```sql
create database ${DB_NAME}
```

4. The benchmark user must already exist in Microsoft SQL Server, and the database corresponding to `DB_NAME` must have the required permissions.
5. In the SQL Server server properties, set the authentication mode to allow SQL Server authentication so the benchmark can connect with username and password.

The following SQL from the original README is an example that creates user `test` with password `12345678` and grants broad server-level permissions. Keep or narrow these permissions according to your environment policy:

```sql
USE [master]
GO
CREATE LOGIN [test] WITH PASSWORD=N'12345678' MUST_CHANGE, DEFAULT_DATABASE=[master], CHECK_EXPIRATION=ON, CHECK_POLICY=ON
GO
ALTER SERVER ROLE [bulkadmin] ADD MEMBER [test]
GO
ALTER SERVER ROLE [dbcreator] ADD MEMBER [test]
GO
ALTER SERVER ROLE [diskadmin] ADD MEMBER [test]
GO
ALTER SERVER ROLE [processadmin] ADD MEMBER [test]
GO
ALTER SERVER ROLE [securityadmin] ADD MEMBER [test]
GO
ALTER SERVER ROLE [serveradmin] ADD MEMBER [test]
GO
ALTER SERVER ROLE [setupadmin] ADD MEMBER [test]
GO
ALTER SERVER ROLE [sysadmin] ADD MEMBER [test]
GO
```

Additional notes for this module:

- `CREATE_SCHEMA=true` creates benchmark tables inside the existing database named by `DB_NAME`; it does not create the database itself.
- `IS_DELETE_DATA=true` drops the benchmark tables created by this module inside `DB_NAME`.

## 3. Build benchmark

Build only the Microsoft SQL Server module and its dependencies:

```bash
mvn -pl mssqlserver -am package -DskipTests
```

This command has been verified locally in this repository.

After packaging, the benchmark tool is generated under:

```text
mssqlserver/target/iot-benchmark-mssqlserver
mssqlserver/target/iot-benchmark-mssqlserver.zip
```

## 4. Configure benchmark

There is a demo configuration file at [config.properties](./config.properties).

For the current `mssqlserver` module, check at least the following items:

| Key         | Required | Description                                                                                                               |
| :---------- | :------- | :------------------------------------------------------------------------------------------------------------------------ |
| `DB_SWITCH` | Yes      | Must be `MSSQLSERVER`.                                                                                                    |
| `HOST`      | Yes      | Target SQL Server host. If multiple hosts are configured in the framework, this module currently uses only the first one. |
| `PORT`      | Yes      | Target SQL Server port. The sample uses `1433`.                                                                           |
| `DB_NAME`   | Yes      | Existing target database name. This module expects the database to exist before startup.                                  |
| `USERNAME`  | Yes      | SQL Server login name used by the JDBC connection.                                                                        |
| `PASSWORD`  | Yes      | Password for the SQL Server login.                                                                                        |

Minimal example:

```properties
DB_SWITCH=MSSQLSERVER
HOST=127.0.0.1
PORT=1433
USERNAME=test
PASSWORD=12345678
DB_NAME=ms
```

Other workload parameters such as `CLIENT_NUMBER`, `LOOP`, `BATCH_SIZE_PER_WRITE`, `OPERATION_PROPORTION`, and `QUERY_INTERVAL` are inherited from the global benchmark configuration template under `configuration/conf/config.properties`.

The current `mssqlserver` module does **not** support the following benchmark features:

- `verificationQueryMode`
- comparison or verification paths that require framework verification support, including `IS_COMPARISON=true` and `IS_POINT_COMPARISON=true`
- `GROUP_BY_DESC` in `OPERATION_PROPORTION`
- `SET_OPERATION` in `OPERATION_PROPORTION`
- `ALIGN_BY_DEVICE=true`
- `RESULT_ROW_LIMIT >= 0`

## 5. Run benchmark

Run the benchmark with the packaged scripts:

```bash
cd mssqlserver/target/iot-benchmark-mssqlserver
./benchmark.sh
```

If you want to run with a custom configuration directory or file path:

```bash
./benchmark.sh -cf conf
```

For a normal mixed read/write benchmark, use `BENCHMARK_WORK_MODE=testWithDefaultPath` and avoid unsupported options listed above.

The test result will be printed in the console and recorded in the generated `logs` directory during execution.

## 6. Test result

```text
----------------------Main Configurations----------------------
DB_SWITCH: MSSQLSERVER
OPERATION_PROPORTION: 1:1:1:1:1:1:1:1:1:1:1
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:1
IS_CLIENT_BIND: true
CLIENT_NUMBER: 5
GROUP_NUMBER: 20
DEVICE_NUMBER: 5
SENSOR_NUMBER: 100
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
Test elapsed time (not include schema creation): 41.12 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           478                 478000              0                   0                   11624.09            
PRECISE_POINT       462                 101                 0                   0                   2.46                
TIME_RANGE          438                 22583               0                   0                   549.18              
VALUE_RANGE         483                 28549               0                   0                   694.26              
AGG_RANGE           425                 2550                0                   0                   62.01               
AGG_VALUE           460                 1840                0                   0                   44.75               
AGG_RANGE_VALUE     431                 1724                0                   0                   41.92               
GROUP_BY            473                 670                 0                   0                   16.29               
LATEST_POINT        459                 622                 0                   0                   15.13               
RANGE_QUERY_DESC    433                 22805               0                   0                   554.58              
VALUE_RANGE_QUERY_DESC458                 27189               0                   0                   661.19              
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           328.01      125.89      267.19      279.30      306.17      346.51      398.88      452.22      822.86      1149.90     1113.72     33317.36    
PRECISE_POINT       2.74        0.97        2.05        2.28        2.52        2.80        3.34        3.91        9.29        20.78       18.40       273.56      
TIME_RANGE          3.06        1.37        2.27        2.53        2.80        3.18        3.63        4.19        7.29        34.54       30.13       302.97      
VALUE_RANGE         2.10        0.85        1.52        1.72        1.90        2.20        2.53        2.91        5.88        34.05       27.18       236.07      
AGG_RANGE           2.96        1.32        2.15        2.40        2.71        3.05        3.42        3.99        6.31        28.60       27.65       285.49      
AGG_VALUE           2.12        0.69        1.31        1.49        1.84        2.23        2.63        3.07        8.87        25.64       25.54       231.55      
AGG_RANGE_VALUE     2.06        0.86        1.55        1.71        1.90        2.10        2.38        2.80        6.15        18.99       17.62       194.84      
GROUP_BY            22.05       5.35        14.10       15.93       17.69       19.71       24.62       58.67       122.97      162.91      156.56      2179.26     
LATEST_POINT        35.01       1.48        19.41       22.87       26.08       30.38       69.80       109.93      167.11      247.34      239.67      3681.73     
RANGE_QUERY_DESC    3.19        1.35        2.23        2.56        2.89        3.29        3.73        4.23        11.27       38.03       33.73       308.78      
VALUE_RANGE_QUERY_DESC2.22        0.74        1.55        1.76        1.99        2.24        2.75        3.67        10.17       16.05       14.87       216.73      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```
