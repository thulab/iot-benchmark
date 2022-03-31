# 1. IoTDB-Benchmark
![](https://img.shields.io/badge/platform-MacOS%20%7C%20Linux%20%7C%20Windows-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)

You can also read [中文版本](README-cn.md).

# 2. Table of Contents

- [1. IoTDB-Benchmark](#1-iotdb-benchmark)
- [2. Table of Contents](#2-table-of-contents)
- [3. Overview](#3-overview)
- [4. Main Features](#4-main-features)
- [5. Usage of IoTDB-Benchmark](#5-usage-of-iotdb-benchmark)
  - [5.1. Prerequisites of IoTDB-Benchmark](#51-prerequisites-of-iotdb-benchmark)
  - [5.2. Working modes of IoTDB-Benchmark](#52-working-modes-of-iotdb-benchmark)
  - [5.3. Build of IoTDB-Benchmark](#53-build-of-iotdb-benchmark)
- [6. Explanation of different operating modes of IoTDB-Benchmark](#6-explanation-of-different-operating-modes-of-iotdb-benchmark)
  - [6.1. Write of Conventional test mode(Single database)](#61-write-of-conventional-test-modesingle-database)
    - [6.1.1. Configure](#611-configure)
    - [6.1.2. Start(Without Server System Information Recording)](#612-startwithout-server-system-information-recording)
    - [6.1.3. Execute](#613-execute)
  - [6.2. Query of Conventional test mode(Single database, not use system records)](#62-query-of-conventional-test-modesingle-database-not-use-system-records)
    - [6.2.1. Configure](#621-configure)
    - [6.2.2. Start](#622-start)
    - [6.2.3. Execute](#623-execute)
  - [6.3. Read and write mixed mode of conventional test mode (Single database)](#63-read-and-write-mixed-mode-of-conventional-test-mode-single-database)
    - [6.3.1. Configure](#631-configure)
    - [6.3.2. Start](#632-start)
    - [6.3.3. Execute](#633-execute)
  - [6.4. Read and write mixed mode of conventional test mode (single database, query the most recently written data)](#64-read-and-write-mixed-mode-of-conventional-test-mode-single-database-query-the-most-recently-written-data)
    - [6.4.1. Configure](#641-configure)
    - [6.4.2. Start](#642-start)
    - [6.4.3. Execute](#643-execute)
  - [6.5. Use system records of conventional test mode (single database)](#65-use-system-records-of-conventional-test-mode-single-database)
    - [6.5.1. Configure](#651-configure)
    - [6.5.2. Start (With Server System Information Recording)](#652-start-with-server-system-information-recording)
  - [6.6. Persistence of the test process in conventional test mode (single database)](#66-persistence-of-the-test-process-in-conventional-test-mode-single-database)
  - [6.7. Generate data mode](#67-generate-data-mode)
    - [6.7.1. Configure](#671-configure)
    - [6.7.2. Start](#672-start)
    - [6.7.3. Execute](#673-execute)
  - [6.8. Write mode for Verificaiton (single database, external data set)](#68-write-mode-for-verificaiton-single-database-external-data-set)
    - [6.8.1. Configure](#681-configure)
    - [6.8.2. Start](#682-start)
    - [6.8.3. Execute](#683-execute)
  - [6.9. Single-point query mode for Verificaiton (single database, external data set)](#69-single-point-query-mode-for-verificaiton-single-database-external-data-set)
    - [6.9.1. Configure](#691-configure)
    - [6.9.2. Start](#692-start)
    - [6.9.3. Execute](#693-execute)
  - [6.10. Dual database mode](#610-dual-database-mode)
  - [6.11. Writing in conventional test mode (dual database)](#611-writing-in-conventional-test-mode-dual-database)
    - [6.11.1. Configure](#6111-configure)
    - [6.11.2. Start](#6112-start)
    - [6.11.3. Excute](#6113-excute)
  - [6.12. Single-point query mode for correctness (dual database comparison)](#612-single-point-query-mode-for-correctness-dual-database-comparison)
    - [6.12.1. Configure](#6121-configure)
    - [6.12.2. Start](#6122-start)
    - [6.12.3. Execute](#6123-execute)
  - [6.13. Correctness function query mode (dual database comparison)](#613-correctness-function-query-mode-dual-database-comparison)
    - [6.13.1. Configure](#6131-configure)
    - [6.13.2. Start](#6132-start)
    - [6.13.3. Execute](#6133-execute)
- [7. Test Other Database(Part)](#7-test-other-databasepart)
  - [7.1. Test InfluxDB v1.x](#71-test-influxdb-v1x)
  - [7.2. Test InfluxDB v2.0](#72-test-influxdb-v20)
  - [7.3. Test Microsoft SQL Server](#73-test-microsoft-sql-server)
  - [7.4. Test QuestDB](#74-test-questdb)
  - [7.5. Test SQLite](#75-test-sqlite)
  - [7.6. Test Victoriametrics](#76-test-victoriametrics)
  - [7.7. Test TimeScaleDB](#77-test-timescaledb)
  - [7.8. Test PI Archive](#78-test-pi-archive)
  - [7.9. Test TDenginee](#79-test-tdenginee)
- [8. Further explanation of correctness verification](#8-further-explanation-of-correctness-verification)
- [9. Perform Multiple Tests Automatically](#9-perform-multiple-tests-automatically)
  - [9.1. Configure routine](#91-configure-routine)
  - [9.2. Start](#92-start)
- [10. Developer Guidelines](#10-developer-guidelines)
- [11. Related Article](#11-related-article)

# 3. Overview

IoTDB-benchmark is a tool for benchmarking IoTDB against other databases and time series solutions.

Databases currently supported:

|       Database       | Version  |                       Insert_Mode                        |
| :------------------: | :------: | :------------------------------------------------------: |
|        IoTDB         |  v0.12   | jdbc、sessionByTablet、sessionByRecord、sessionByRecords |
|        IoTDB         |  v0.11   |                jdbc、session、sessionPool                |
|        IoTDB         |  v0.10   |                      jdbc、session                       |
|        IoTDB         |   v0.9   |                      jdbc、session                       |
|       InfluxDB       |   v1.x   |                           SDK                            |
|       InfluxDB       |   v2.0   |                           SDK                            |
|       QuestDB        |  v6.0.7  |                           jdbc                           |
| Microsoft SQL Server | 2016 SP2 |                           jdbc                           |
|   VictoriaMetrics    | v1.64.0  |                       Http Request                       |
|        SQLite        |    --    |                           jdbc                           |
|       OpenTSDB       |    2.4.1    |                       Http Request                       |
|       KairosDB       |    --    |                       Http Request                       |
|     TimescaleDB      |    --    |                           jdbc                           |
|        TDengine        |    2.2.0.2    |                           jdbc                           |
|       PI Archive     |    2016     |                           jdbc                        |

# 4. Main Features

IoTDB-benchmark's features are as following:

1. Easy to use: IoTDB-benchmark is a tool combined multiple testing functions so users do not need to switch different tools. 
2. Various data ingestion and testing mode:
   1. Generate periodic time series data according to the configuration and insert and query directly.
   2. Write the generated data to the corresponding location on the disk.
   3. Load data from the generated data set generated in the disk, and write and query.
   4. Perform correctness verification tests on data and query results respectively.
3. Testing report and result: Supporting storing testing information and results for further query or analysis.
4. Visualize test results: Integration with Tableau to visualize the test result.
5. We recommend using MacOs or Linux systems. This article takes MacOS and Linux systems as examples. If you use Windows systems, please use the `benchmark.bat` script in the `conf` folder to start the benchmark.

# 5. Usage of IoTDB-Benchmark

## 5.1. Prerequisites of IoTDB-Benchmark

To use IoTDB-benchmark, you need to have:

1. Java 8
2. Maven: It is not recommended to use the mirror.
3. The appropriate version of the database
   1. Apache IoTDB >= v0.9 ([Get it!](https://github.com/apache/iotdb))，now mainly supported IoTDB v0.12
   2. His corresponding version of the database
4. ServerMode and CSV recording modes can only be used in Linux systems to record relevant system information during the test.

## 5.2. Working modes of IoTDB-Benchmark
|           The name of mode            |  BENCHMARK_WORK_MODE  | The content of mode                                                                                                                              |
| :-----------------------------------: | :-------------------: | :----------------------------------------------------------------------------------------------------------------------------------------------- |
|        Conventional test mode         |  testWithDefaultPath  | Supports mixed loads of multiple read and write operations                                                                                       |
|          Generate data mode           |   generateDataMode    | Benchmark generates the data set to the FILE_PATH path                                                                                           |
|      Write mode of verification       | verificationWriteMode | Load the data set from the FILE_PATH path for writing, currently supports IoTDB v0.12                                                            |
|      Query mode of verification       | verificationQueryMode | Load the data set from the FILE_PATH path and compare it with the database. Currently, IoTDB v0.12 is supported                                  |
| Server resource usage monitoring mode |      serverMODE       | Server resource usage monitoring mode (run in this mode is started by the ser-benchmark.sh script, no need to manually configure this parameter) |

## 5.3. Build of IoTDB-Benchmark

You can build IoTDB-benchmark using Maven, running following command in the **root** of project:

```
mvn clean package -Dmaven.test.skip=true
```

This will compile all versions of IoTDB and other database benchmark. if you want to compile a specific database, go to the package and run above command.

After, for example, you can go to `/iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`, and run `./benchmark.sh` to start IoTDB-Benchmark.

The default configuration file is stored under `iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1/conf`, you can edit `config.properties` to complete the configuration, please **note that you need Adjust the DB_SWITCH parameter in the configuration file to the database you need to be tested**. The corresponding relationship and possible values are as follows:

|       Database       | Version  | Corresponding Sub-project |                                                  DB_SWITCH                                                  |
| :------------------: | :------: | :-----------------------: | :---------------------------------------------------------------------------------------------------------: |
|        IoTDB         |   0.12   |        iotdb-0.12         | IoTDB-012-JDBC<br>IoTDB-012-SESSION_BY_TABLET<br>IoTDB-012-SESSION_BY_RECORD<br>IoTDB-012-SESSION_BY_RECORDS |
|        IoTDB         |   0.11   |        iotdb-0.11         |                        IoTDB-011-JDBC<br>IoTDB-011-SESSION<br>IoTDB-011-SESSION_POOL                        |
|        IoTDB         |   0.10   |        iotdb-0.10         |                                     IoTDB-010-JDBC<br>IoTDB-010-SESSION                                     |
|        IoTDB         |   0.9    |        iotdb-0.09         |                                      IoTDB-09-JDBC<br>IoTDB-09-SESSION                                      |
|       InfluxDB       |   v1.x   |         influxdb          |                                                  InfluxDB                                                   |
|       InfluxDB       |   v2.0   |       influxdb-2.0        |                                                InfluxDB-2.0                                                 |
|       QuestDB        |  v6.0.7  |          questdb          |                                                   QuestDB                                                   |
| Microsoft SQL Server | 2016 SP2 |        mssqlserver        |                                                 MSSQLSERVER                                                 |
|   VictoriaMetrics    | v1.64.0  |      victoriametrics      |                                               VictoriaMetrics                                               |
|     TimescaleDB      |    --    |        timescaledb        |                                                 TimescaleDB                                                 |
|        SQLite        |    --    |          sqlite           |                                                   SQLite                                                    |
|       OpenTSDB       |    2.4.1    |         opentsdb          |                                                  OpenTSDB                                                   |
|       KairosDB       |    --    |         kairosdb          |                                                  KairosDB                                                   |
|        TDengine        |    2.2.0.2    |          TDengine           |                                                   TDengine                                                    |
|       PI Archive      |   2016  |         PIArchive         |                                                     PIArchive                                                |

# 6. Explanation of different operating modes of IoTDB-Benchmark

This short guide will walk you through the basic process of using IoTDB-benchmark.

## 6.1. Write of Conventional test mode(Single database)

### 6.1.1. Configure

Before starting any new test case, you need to config the configuration files ```config.properties``` first. For your convenience, we have already set the default config for the following demonstration.

Suppose you are going to test data ingestion performance of IoTDB. You have installed IoTDB dependencies and launched a IoTDB server with default settings. The IP of the server is 127.0.0.1. Suppose the workload parameters are:

```
+--------------------------+------------+--------------+-------------+------------+-------------------+------+
| Measurement/StorageGroup | tag/device | field/sensor | concurrency | batch size | point interval(s) | loop |
+--------------------------+------------+--------------+-------------+------------+-------------------+------+
|            20            |    20      |      300     |     20      |      1     |         5         | 1000 |
+--------------------------+------------+--------------+-------------+------------+-------------------+------+
```

edit the corresponding parameters in the ```config.properties``` file as following:

```properties
HOST=127.0.0.1
PORT=6667
DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
BENCHMARK_WORK_MODE=testWithDefaultPath
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
GROUP_NUMBER=20
DEVICE_NUMBER=20
SENSOR_NUMBER=300
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=1
POINT_STEP=5000
LOOP=1000
```

Currently, you can edit other configs, more config in [config.properties](configuration/conf/config.properties)

### 6.1.2. Start(Without Server System Information Recording)

Before running the test, you need to open the IoTDB service on port 6667.

Then, you can go to `/iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.1.3. Execute 

Now after launching the test, you will see testing information rolling like following: 

```
...
19:39:51.392 [pool-16-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-5 20.30% workload is done.
19:39:51.392 [pool-57-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-19 19.50% workload is done.
19:39:51.392 [pool-40-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-13 20.50% workload is done.
19:39:51.392 [pool-43-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-14 23.80% workload is done.
...
```

When test is done, the last output of the test information will be like following: 

```
19:39:52.794 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.BaseMode - All clients finished.
----------------------Main Configurations----------------------
CREATE_SCHEMA=true
START_TIME=2018-9-20T00:00:00+08:00
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
BATCH_SIZE_PER_WRITE=1
IS_CLIENT_BIND=true
LOOP=1000
IS_OUT_OF_ORDER=false
IS_REGULAR_FREQUENCY=true
GROUP_NUMBER=20
QUERY_INTERVAL=250000
SENSOR_NUMBER=300
RESULT_PRECISION=0.1%
POINT_STEP=5000
CLIENT_NUMBER=20
SG_STRATEGY=mod
REAL_INSERT_RATE=1.0
OUT_OF_ORDER_MODE=POISSON
DBConfig=
  DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
  HOST=[127.0.0.1]
  PORT=[6667]
  USERNAME=root
  PASSWORD=root
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=false
BENCHMARK_WORK_MODE=testWithDefaultPath
OP_INTERVAL=0
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
DEVICE_NUMBER=20
OUT_OF_ORDER_RATIO=0.5
BENCHMARK_CLUSTER=false
IS_DELETE_DATA=true
IS_SENSOR_TS_ALIGNMENT=true
---------------------------------------------------------------
main measurements:
Create schema cost 0.21 second
Test elapsed time (not include schema creation): 2.42 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           20000               6000000             0                   0                   2484231.33          
PRECISE_POINT       0                   0                   0                   0                   0.00                
TIME_RANGE          0                   0                   0                   0                   0.00                
VALUE_RANGE         0                   0                   0                   0                   0.00                
AGG_RANGE           0                   0                   0                   0                   0.00                
AGG_VALUE           0                   0                   0                   0                   0.00                
AGG_RANGE_VALUE     0                   0                   0                   0                   0.00                
GROUP_BY            0                   0                   0                   0                   0.00                
LATEST_POINT        0                   0                   0                   0                   0.00                
RANGE_QUERY_DESC    0                   0                   0                   0                   0.00                
VALUE_RANGE_QUERY_DESC0                   0                   0                   0                   0.00                
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           2.25        0.40        0.54        0.72        1.08        1.62        2.98        4.65        32.52       106.00      185.18      2288.93     
PRECISE_POINT       0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
TIME_RANGE          0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE         0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_RANGE           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_VALUE           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_RANGE_VALUE     0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
GROUP_BY            0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
LATEST_POINT        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
RANGE_QUERY_DESC    0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE_QUERY_DESC0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

The output contains overall information of the test including:
+ Main configurations
+ Total elapse time during the test
+ Time cost of schema creation
  + okOperation: successfully executed request/SQL number for different operations
  + okPoint: successfully ingested data point number or successfully returned query result point number
  + failOperation: the request/SQL number failed to execute for different operations
  + failPoint: the data point number failed to ingest (for query operations currently this field is always zero)
  + throughput: equals to ```okPoint / Test elapsed time```
  + <a href = "https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=199529657">Detailed parameter description</a>
+ The latency statistics of different operations in millisecond 
  + ```SLOWEST_THREAD``` is the max accumulative operation time-cost among the client threads

All these information will be logged in ```logs``` directory on client server.

If you want to write data out of order, you need to modify the following properties of the `config.properties` file:

```
# Whether write out of order data
IS_OUT_OF_ORDER=true
# Out-of-order write mode, currently there are 2 types as follows
# POISSION Out of order mode according to Poisson distribution
# BATCH Batch insert out of order mode
OUT_OF_ORDER_MODE=BATCH
# Proportion of data written out of order
OUT_OF_ORDER_RATIO=0.5
# Is the time stamp of equal length
IS_REGULAR_FREQUENCY=true
# Poisson distribution expectation and variance
LAMBDA=2200.0
# The maximum value of the random number of the Poisson distribution model
MAX_K=170000
```


Till now, we have already complete the writing test case without server information recording. If you need to use to complete other tests, please continue reading.

## 6.2. Query of Conventional test mode(Single database, not use system records)

In addition to writing data, the conventional test mode can also query data. In addition, this mode also supports mixed read and write operations.

### 6.2.1. Configure

Edit the corresponding parameters in the ```config.properties``` file as following(**Notice**: set ```IS_DELETE_DATA=false``` to close the clean of data):

```properties
### Main Data Ingestion and Query Shared Parameters
HOST=127.0.0.1
PORT=6667
IS_DELETE_DATA=false
DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
BENCHMARK_WORK_MODE=testWithDefaultPath
OPERATION_PROPORTION=0:1:1:1:1:1:1:1:1:1:1
GROUP_NUMBER=20
DEVICE_NUMBER=20
SENSOR_NUMBER=300
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=1
POINT_STEP=5000
# the operation number executed by each client thread
LOOP=1000

### Main Query Related Parameters
# the number of sensor involved in each query request or SQL 
QUERY_SENSOR_NUM=1
# the number of device involved in each query request or SQL 
QUERY_DEVICE_NUM=1
# the aggregation function for aggregate query
QUERY_AGGREGATE_FUN=count
# the variation step of time range query condition for different operation epoch
STEP_SIZE=1
# the time range interval of time range query condition
QUERY_INTERVAL=250000
# the aggregation granularity of group-by (down-sampling) query
GROUP_BY_TIME_UNIT=20000
```

> NOTE:
>
> The `config.properties` contains query-related configuration parameters, which you can learn by viewing the sample file.

### 6.2.2. Start

Before running the test, you need to open the IoTDB service on port 6667.

Then, you can go to `/iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.2.3. Execute 

Now after launching the test, you will see testing information rolling like following: 

```
...
19:11:17.059 [pool-33-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.generate.GenerateBaseClient - pool-4-thread-10 86.10% syntheticWorkload is done.
19:11:17.059 [pool-21-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.generate.GenerateBaseClient - pool-4-thread-6 87.70% syntheticWorkload is done.
19:11:17.059 [pool-48-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.generate.GenerateBaseClient - pool-4-thread-15 75.00% syntheticWorkload is done.
...
```

When test is done, the last testing information will be like the following: 

```
19:42:43.000 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.BaseMode - All clients finished.
----------------------Main Configurations----------------------
CREATE_SCHEMA=false
START_TIME=2018-9-20T00:00:00+08:00
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
BATCH_SIZE_PER_WRITE=1
IS_CLIENT_BIND=true
LOOP=1000
IS_OUT_OF_ORDER=false
IS_REGULAR_FREQUENCY=true
GROUP_NUMBER=20
QUERY_INTERVAL=250000
SENSOR_NUMBER=300
RESULT_PRECISION=0.1%
POINT_STEP=5000
CLIENT_NUMBER=20
SG_STRATEGY=mod
REAL_INSERT_RATE=1.0
OUT_OF_ORDER_MODE=POISSON
DBConfig=
  DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
  HOST=[127.0.0.1]
  PORT=[6667]
  USERNAME=root
  PASSWORD=root
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=false
BENCHMARK_WORK_MODE=testWithDefaultPath
OP_INTERVAL=0
OPERATION_PROPORTION=0:1:1:1:1:1:1:1:1:1:1
DEVICE_NUMBER=20
OUT_OF_ORDER_RATIO=0.5
BENCHMARK_CLUSTER=false
IS_DELETE_DATA=false
IS_SENSOR_TS_ALIGNMENT=true
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 1.60 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           0                   0                   0                   0                   0.00                
PRECISE_POINT       2011                1991                0                   0                   1247.48             
TIME_RANGE          1983                101124              0                   0                   63360.14            
VALUE_RANGE         1964                100156              0                   0                   62753.63            
AGG_RANGE           2042                2042                0                   0                   1279.43             
AGG_VALUE           1912                1912                0                   0                   1197.98             
AGG_RANGE_VALUE     1977                1977                0                   0                   1238.71             
GROUP_BY            2017                26221               0                   0                   16429.00            
LATEST_POINT        2070                2070                0                   0                   1296.98             
RANGE_QUERY_DESC    2056                104845              0                   0                   65691.57            
VALUE_RANGE_QUERY_DESC1968                100356              0                   0                   62878.94            
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT       1.36        0.57        0.66        0.71        0.86        1.23        2.35        3.25        8.13        21.37       53.07       184.71      
TIME_RANGE          1.43        0.60        0.69        0.75        0.89        1.25        2.25        3.27        10.60       45.40       53.19       184.30      
VALUE_RANGE         1.44        0.60        0.71        0.76        0.88        1.22        2.11        2.90        13.74       42.46       43.02       232.00      
AGG_RANGE           1.25        0.56        0.66        0.71        0.83        1.13        1.95        2.63        9.24        24.54       43.21       157.19      
AGG_VALUE           1.47        0.73        0.84        0.90        1.04        1.33        2.25        3.11        8.56        28.29       35.07       210.58      
AGG_RANGE_VALUE     1.45        0.70        0.78        0.84        0.98        1.27        2.02        3.04        8.72        43.52       50.86       199.38      
GROUP_BY            1.28        0.54        0.64        0.70        0.83        1.16        1.96        2.90        6.94        36.30       43.57       233.86      
LATEST_POINT        1.53        0.37        0.49        0.54        0.65        0.94        1.57        2.55        31.88       53.67       53.70       206.66      
RANGE_QUERY_DESC    1.40        0.61        0.70        0.75        0.89        1.24        2.07        2.87        9.61        42.87       75.45       204.70      
VALUE_RANGE_QUERY_DESC1.40        0.61        0.72        0.76        0.89        1.21        2.03        3.14        10.59       48.94       52.89       177.61      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

> Note: 
>
> When okOperation is smaller than 1000 or 100, the quantiles P99 and P999 may even bigger than MAX because we use the T-Digest Algorithm which uses interpolation in that scenario. 

## 6.3. Read and write mixed mode of conventional test mode (Single database)

The conventional test mode can support users to perform mixed reading and writing tests. It should be noted that the timestamps for mixed reading and writing in this scenario start from **the start time of writing**.

### 6.3.1. Configure

Modify the relevant parameters in the ```config.properties``` file as follows (pay special attention to setting ```IS_RECENT_QUERY=false``` to close the recent query mode):

```properties
### Main Data Ingestion and Query Shared Parameters
HOST=127.0.0.1
PORT=6667
IS_DELETE_DATA=false
DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
BENCHMARK_WORK_MODE=testWithDefaultPath
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1
GROUP_NUMBER=20
DEVICE_NUMBER=20
SENSOR_NUMBER=300
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=1
POINT_STEP=5000
LOOP=1000
IS_RECENT_QUERY=false

### Main Query Related Parameters
# the number of sensor involved in each query request or SQL 
QUERY_SENSOR_NUM=1
# the number of device involved in each query request or SQL 
QUERY_DEVICE_NUM=1
# the aggregation function for aggregate query
QUERY_AGGREGATE_FUN=count
# the variation step of time range query condition for different operation epoch
STEP_SIZE=1
# the time range interval of time range query condition
QUERY_INTERVAL=250000
# the aggregation granularity of group-by (down-sampling) query
GROUP_BY_TIME_UNIT=20000
```

### 6.3.2. Start

Before starting the test, you need to start the IoTDB service on port 6667 of the machine.

Then you enter `iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1` and run the following command to start Benchmark (currently only execute the following script in Unix/OS X system):

```sh
> ./benchmark.sh
```

### 6.3.3. Execute

After the test is started, you can see the rolling test execution information, some of which are as follows:

```
...
19:54:38.983 [pool-16-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-5 84.30% workload is done.
19:54:38.983 [pool-55-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-18 86.00% workload is done.
19:54:38.983 [pool-40-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-13 84.00% workload is done.
19:54:38.983 [pool-46-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-15 85.10% workload is done.
...
```

When the test is over, the statistical information of this test will be displayed at last, as shown below:

```
19:54:39.178 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.BaseMode - All clients finished.
----------------------Main Configurations----------------------
CREATE_SCHEMA=true
START_TIME=2018-9-20T00:00:00+08:00
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
BATCH_SIZE_PER_WRITE=1
IS_CLIENT_BIND=true
LOOP=1000
IS_OUT_OF_ORDER=false
IS_REGULAR_FREQUENCY=true
GROUP_NUMBER=20
IS_RECENT_QUERY=false
QUERY_INTERVAL=250000
SENSOR_NUMBER=300
RESULT_PRECISION=0.1%
POINT_STEP=5000
CLIENT_NUMBER=20
SG_STRATEGY=mod
REAL_INSERT_RATE=1.0
OUT_OF_ORDER_MODE=POISSON
DBConfig=
  DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
  HOST=[127.0.0.1]
  PORT=[6667]
  USERNAME=root
  PASSWORD=root
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=false
BENCHMARK_WORK_MODE=testWithDefaultPath
OP_INTERVAL=0
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1
DEVICE_NUMBER=20
OUT_OF_ORDER_RATIO=0.5
BENCHMARK_CLUSTER=false
IS_DELETE_DATA=true
IS_SENSOR_TS_ALIGNMENT=true
---------------------------------------------------------------
main measurements:
Create schema cost 0.13 second
Test elapsed time (not include schema creation): 1.20 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           1824                547200              0                   0                   454490.46           
PRECISE_POINT       1831                0                   0                   0                   0.00                
TIME_RANGE          1776                0                   0                   0                   0.00                
VALUE_RANGE         1871                0                   0                   0                   0.00                
AGG_RANGE           1734                1734                0                   0                   1440.22             
AGG_VALUE           1769                1769                0                   0                   1469.29             
AGG_RANGE_VALUE     1790                1790                0                   0                   1486.73             
GROUP_BY            1912                24856               0                   0                   20644.76            
LATEST_POINT        1841                1811                0                   0                   1504.17             
RANGE_QUERY_DESC    1875                0                   0                   0                   0.00                
VALUE_RANGE_QUERY_DESC1777                0                   0                   0                   0.00                
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           1.19        0.48        0.54        0.57        0.62        0.79        1.40        1.70        19.96       42.62       46.36       145.92      
PRECISE_POINT       0.93        0.37        0.43        0.47        0.55        0.74        1.09        1.91        7.26        45.30       70.77       147.95      
TIME_RANGE          0.98        0.38        0.44        0.48        0.56        0.78        1.11        1.91        10.37       33.01       34.32       125.31      
VALUE_RANGE         0.93        0.38        0.43        0.47        0.54        0.72        1.04        1.81        8.20        36.80       85.32       143.19      
AGG_RANGE           1.00        0.43        0.51        0.55        0.63        0.82        1.22        1.96        9.45        24.39       28.49       149.08      
AGG_VALUE           1.04        0.49        0.57        0.61        0.70        0.89        1.25        2.04        9.12        21.21       23.62       125.92      
AGG_RANGE_VALUE     1.04        0.45        0.53        0.57        0.66        0.85        1.21        1.94        8.84        36.21       85.38       175.98      
GROUP_BY            1.03        0.45        0.52        0.56        0.64        0.83        1.21        2.30        8.87        23.33       38.16       143.27      
LATEST_POINT        1.29        0.41        0.47        0.52        0.59        0.79        1.19        2.56        35.24       36.42       58.61       161.28      
RANGE_QUERY_DESC    0.88        0.38        0.44        0.48        0.56        0.75        1.06        1.71        7.69        23.27       23.31       127.48      
VALUE_RANGE_QUERY_DESC0.96        0.37        0.44        0.48        0.55        0.72        1.13        2.03        7.93        32.28       119.40      182.66      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 6.4. Read and write mixed mode of conventional test mode (single database, query the most recently written data)

The conventional test mode can support the user to perform a mixed read-write test (query the most recently written data). It should be noted that the query time range in this scenario is the data adjacent to the **left side** of the current maximum write timestamp.

### 6.4.1. Configure

Modify the relevant parameters in the ```config.properties``` file as follows (pay special attention to setting ```IS_RECENT_QUERY=true``` to close the recent query mode):

```properties
### Main Data Ingestion and Query Shared Parameters
HOST=127.0.0.1
PORT=6667
IS_DELETE_DATA=false
DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
BENCHMARK_WORK_MODE=testWithDefaultPath
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1
GROUP_NUMBER=20
DEVICE_NUMBER=20
SENSOR_NUMBER=300
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=1
POINT_STEP=5000
LOOP=1000
IS_RECENT_QUERY=true

### Main Query Related Parameters
# the number of sensor involved in each query request or SQL 
QUERY_SENSOR_NUM=1
# the number of device involved in each query request or SQL 
QUERY_DEVICE_NUM=1
# the aggregation function for aggregate query
QUERY_AGGREGATE_FUN=count
# the variation step of time range query condition for different operation epoch
STEP_SIZE=1
# the time range interval of time range query condition
QUERY_INTERVAL=250000
# the aggregation granularity of group-by (down-sampling) query
GROUP_BY_TIME_UNIT=20000
```

### 6.4.2. Start

Before starting the test, you need to start the IoTDB service on port 6667 of the machine.

Then you enter `iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1` and run the following command to start Benchmark (currently only execute the following script in Unix/OS X system):

```sh
> ./benchmark.sh
```

### 6.4.3. Execute

After the test is started, you can see the rolling test execution information, some of which are as follows:

```
...
19:56:24.503 [pool-52-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-17 73.90% workload is done.
19:56:24.503 [pool-34-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-11 73.60% workload is done.
19:56:24.503 [pool-40-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-13 75.20% workload is done.
19:56:24.503 [pool-46-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-15 80.50% workload is done.
...
```

When the test is over, the statistical information of this test will be displayed at last, as shown below:

```
19:56:24.835 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.BaseMode - All clients finished.
----------------------Main Configurations----------------------
CREATE_SCHEMA=true
START_TIME=2018-9-20T00:00:00+08:00
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
BATCH_SIZE_PER_WRITE=1
IS_CLIENT_BIND=true
LOOP=1000
IS_OUT_OF_ORDER=false
IS_REGULAR_FREQUENCY=true
GROUP_NUMBER=20
IS_RECENT_QUERY=true
QUERY_INTERVAL=250000
SENSOR_NUMBER=300
RESULT_PRECISION=0.1%
POINT_STEP=5000
CLIENT_NUMBER=20
SG_STRATEGY=mod
REAL_INSERT_RATE=1.0
OUT_OF_ORDER_MODE=POISSON
DBConfig=
  DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
  HOST=[127.0.0.1]
  PORT=[6667]
  USERNAME=root
  PASSWORD=root
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=false
BENCHMARK_WORK_MODE=testWithDefaultPath
OP_INTERVAL=0
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1
DEVICE_NUMBER=20
OUT_OF_ORDER_RATIO=0.5
BENCHMARK_CLUSTER=false
IS_DELETE_DATA=true
IS_SENSOR_TS_ALIGNMENT=true
---------------------------------------------------------------
main measurements:
Create schema cost 0.16 second
Test elapsed time (not include schema creation): 1.34 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           1824                547200              0                   0                   407810.64           
PRECISE_POINT       1831                985                 0                   0                   734.09              
TIME_RANGE          1776                59868               0                   0                   44617.70            
VALUE_RANGE         1871                61613               0                   0                   45918.20            
AGG_RANGE           1734                1734                0                   0                   1292.29             
AGG_VALUE           1769                1769                0                   0                   1318.38             
AGG_RANGE_VALUE     1790                1790                0                   0                   1334.03             
GROUP_BY            1912                24856               0                   0                   18524.38            
LATEST_POINT        1841                1808                0                   0                   1347.44             
RANGE_QUERY_DESC    1875                61920               0                   0                   46146.99            
VALUE_RANGE_QUERY_DESC1777                59887               0                   0                   44631.86            
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           1.02        0.47        0.55        0.58        0.62        0.71        0.94        1.56        12.18       38.28       52.47       123.56      
PRECISE_POINT       1.11        0.37        0.51        0.58        0.70        0.94        1.60        2.69        8.98        23.99       50.06       176.81      
TIME_RANGE          1.27        0.50        0.59        0.66        0.78        1.09        2.03        3.16        9.32        32.81       34.42       142.20      
VALUE_RANGE         1.13        0.48        0.59        0.65        0.75        0.98        1.71        2.72        6.24        32.12       57.05       142.92      
AGG_RANGE           1.09        0.45        0.54        0.61        0.71        0.95        1.61        2.79        8.13        19.74       24.52       133.49      
AGG_VALUE           1.15        0.49        0.60        0.67        0.76        0.96        1.58        2.61        9.61        24.18       27.66       145.30      
AGG_RANGE_VALUE     1.23        0.51        0.61        0.68        0.78        1.02        1.75        2.78        9.99        33.98       51.08       163.81      
GROUP_BY            1.13        0.45        0.56        0.62        0.73        0.94        1.59        2.63        8.72        21.08       29.40       149.96      
LATEST_POINT        1.39        0.40        0.50        0.57        0.68        0.93        1.65        3.31        28.51       30.74       36.17       161.25      
RANGE_QUERY_DESC    1.26        0.49        0.61        0.68        0.79        1.07        1.91        3.21        9.69        22.97       34.34       161.65      
VALUE_RANGE_QUERY_DESC1.16        0.49        0.60        0.67        0.76        1.01        1.70        2.69        9.41        19.58       20.78       127.89      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 6.5. Use system records of conventional test mode (single database)

IoTDB Benchmark allows you to use a database to store system data during the test, and currently supports the use of CSV records.

### 6.5.1. Configure

Suppose your IoTDB server IP is 192.168.130.9 and your test client server which installed IoTDB-benchmark has authorized ssh access to the IoTDB server.
Current version of information recording is dependent on iostat. Please make sure iostat is installed in IoTDB server.

Configure ```config.properties```
Suppose you are using the same parameters as in [Write of conventional test mode](#61-write-of-conventional-test-mode). The new parameters you should add are TEST_DATA_PERSISTENCE and MONITOR_INTERVAL like:

```properties
TEST_DATA_PERSISTENCE=CSV
MONITOR_INTERVAL=0
```

> 1. TEST_DATA_PERSISTENCE=CSV means the test results save as a CSV file.
> 2. INTERVAL=0 means the server information recording with the minimal interval 2 seconds. If you set INTERVAL=n then the interval will be n+2 seconds since the recording process require least 2 seconds. You may want to set the INTERVAL longer when conducting long testing.

### 6.5.2. Start (With Server System Information Recording)

Before running the test, you need to open the IoTDB service on port 6667.

Then, you can go to `/iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

The test results will be saved in the path ```data``` in CSV format

## 6.6. Persistence of the test process in conventional test mode (single database)

For subsequent analysis, IoTDB-Benchmark can store test information in the database (if you don't want to store test data, then set ```TEST_DATA_PERSISTENCE=None```)

The currently supported storage databases are IoTDB and MySQL. Taking MySQL as an example, you need to modify the following configuration in the ```config.properties``` file:

```properties
TEST_DATA_PERSISTENCE=MySQL
# IP address of the database
TEST_DATA_STORE_IP=127.0.0.1
# The port number of the database
TEST_DATA_STORE_PORT=6667
# The name of the database
TEST_DATA_STORE_DB=result
# The username of database
TEST_DATA_STORE_USER=root
# The password of database
TEST_DATA_STORE_PW=root
# Database read timeout, in milliseconds
TEST_DATA_WRITE_TIME_OUT=300000
# Maximum limit of database write concurrent pool
TEST_DATA_MAX_CONNECTION=1
# The remarks of this experiment are stored in the database (such as MySQL) as part of the table name, and be careful not to have special characters such as.
REMARK=
```

Follow-up operations are the same as above.

## 6.7. Generate data mode

### 6.7.1. Configure

In order to generate a data set that can be reused, IoTDB-Benchmark provides a data set generation mode, and the data set is generated to FILE_PATH for subsequent use in the correctness write mode and correctness query mode.

For this, you need to modify the following configuration in ```config.properties```:

```
BENCHMARK_WORK_MODE=generateDataMode
# Data set storage location
FILE_PATH=data/test
DEVICE_NUMBER=5
SENSOR_NUMBER=10
CLIENT_NUMBER=5
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
# The number of batches contained in each data file
BIG_BATCH_SIZE=100
```

> Note:
> The FILE_PATH folder should be an empty folder. If it is not empty, an error will be reported and the generated data set will be stored in this folder.

### 6.7.2. Start

Then, you can go to `/iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.7.3. Execute

Now after launching the test, you will see testing information rolling like following: 

```
...
11:26:43.104 [pool-4-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-3 83.70% workload is done.
11:26:43.104 [pool-2-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-1 83.80% workload is done.
11:26:43.104 [pool-3-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-2 83.40% workload is done.
11:26:43.104 [pool-5-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-4 83.70% workload is done.
11:26:43.104 [pool-6-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-5 83.40% workload is done.
...
```

When test is done, the last testing information will be like the following: 

```
11:26:43.286 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.GenerateDataMode - Data Location: data/test
11:26:43.287 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.GenerateDataMode - Schema Location: data/test/schema.txt
11:26:43.287 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.GenerateDataMode - Generate Info Location: data/test/info.txt
```

> Note:
> 1. The data storage location is under the FILE_PATH folder, and its directory structure is /d_xxx/batch_xxx.txt
> 2. The metadata of the device and sensor is stored in FILE_PATH/schema.txt
> 3. The relevant information of the data set is stored in FILE_PATH/info.txt

The following is an example of info.txt:

```
LOOP=1000
BIG_BATCH_SIZE=100
FIRST_DEVICE_INDEX=0
POINT_STEP=5000
TIMESTAMP_PRECISION='ms'
STRING_LENGTH=2
INSERT_DATATYPE_PROPORTION='1:1:1:1:1:1'
DEVICE_NUMBER=5
REAL_INSERT_RATE=1.0
SENSOR_NUMBER=10
IS_SENSOR_TS_ALIGNMENT=true
DATA_SEED=666
SG_STRATEGY='mod'
GROUP_NUMBER=20
BATCH_SIZE_PER_WRITE=10
START_TIME='2018-9-20T00:00:00+08:00'
IS_OUT_OF_ORDER=false
OUT_OF_ORDER_MODE=POISSON
OUT_OF_ORDER_RATIO=0.5
IS_REGULAR_FREQUENCY=true
LAMBDA=2200.0
MAX_K=170000
STEP_SIZE=1
QUERY_SENSOR_NUM=1
QUERY_DEVICE_NUM=1
QUERY_AGGREGATE_FUN='count'
QUERY_INTERVAL=250000
QUERY_LOWER_VALUE=-5.0
GROUP_BY_TIME_UNIT=20000
QUERY_SEED=151658
QUERY_LIMIT_N=5
QUERY_LIMIT_OFFSET=5
QUERY_SLIMIT_N=5
QUERY_SLIMIT_OFFSET=5
WORKLOAD_BUFFER_SIZE=100
SENSORS=[s_0, s_1, s_2, s_3, s_4, s_5, s_6, s_7, s_8, s_9]
```

## 6.8. Write mode for Verificaiton (single database, external data set)

In order to verify the correctness of the data set writing, you can use this mode to write the data set generated in the generated data mode. Currently this mode only supports IoTDB v0.12

### 6.8.1. Configure

For this, you need to modify the following configuration in ```config.properties```:

```
BENCHMARK_WORK_MODE=verificationWriteMode
# Data set storage location
FILE_PATH=data/test
# The number of batches contained in each data file
BIG_BATCH_SIZE=100
```

Notice:
> 1. The FILE_PATH folder should be the data set generated using the generated data mode
> 2. When running this mode, other parameters should be consistent with the description in info.txt**

If you want to use external data set to write into database, you need to the following configuration in ```config.properties```:
```
BENCHMARK_WORK_MODE=verificationWriteMode
# the file path that you store external data set in. 
FILE_PATH=data/test
# since using one external csv file as data set, BIG_BATCH_SIZE is regarded as 1. 
BIG_BATCH_SIZE=1
# we can't write data in parallel in copy data mode, so CLIENT_NUM should be set 1.
CLIENT_NUM=1
# set to intercept the original CSV data set of BATCH_SIZE_PER_WRITE size
BATCH_SIZE_PER_WRITE=100
# enable the copy mode.
IS_COPY_MODE=true
```
Then you need add the csv file in the FILE_PATH:
```
+ FILE_PATH
   + d_0
       + *.csv  # modify the schema(first line of csv) as  "Sensor,s_0,s_1,..."
   + schema.txt # specify which sensorType of one senor with one row, the content is like "d_0 s_0 3\n d_0 s_1 4"
```
After that, you can start it.

### 6.8.2. Start

Before running the test, you need to open the IoTDB service on port 6667.

Then, you can go to `/iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.8.3. Execute
Now after launching the test, you will see testing information rolling like following:

```
...
11:30:02.615 [pool-9-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-3 55.60% workload is done.
11:30:02.615 [pool-6-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-2 55.40% workload is done.
11:30:02.615 [pool-12-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-4 54.20% workload is done.
11:30:02.615 [pool-4-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-1 55.00% workload is done.
11:30:02.615 [pool-15-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-5 55.00% workload is done.
...
```

When test is done, the last testing information will be like the following:

```
11:30:03.155 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.BaseMode - All clients finished.
----------------------Main Configurations----------------------
CREATE_SCHEMA=true
START_TIME=2018-9-20T00:00:00+08:00
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
BATCH_SIZE_PER_WRITE=10
IS_CLIENT_BIND=true
LOOP=1000
IS_OUT_OF_ORDER=false
IS_REGULAR_FREQUENCY=true
GROUP_NUMBER=20
IS_RECENT_QUERY=true
QUERY_INTERVAL=250000
SENSOR_NUMBER=10
RESULT_PRECISION=0.1%
POINT_STEP=5000
CLIENT_NUMBER=5
SG_STRATEGY=mod
REAL_INSERT_RATE=1.0
OUT_OF_ORDER_MODE=POISSON
DBConfig=
  DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
  HOST=[127.0.0.1]
  PORT=[6667]
  USERNAME=root
  PASSWORD=root
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=false
BENCHMARK_WORK_MODE=verificationWriteMode
OP_INTERVAL=0
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1
DEVICE_NUMBER=5
OUT_OF_ORDER_RATIO=0.5
BENCHMARK_CLUSTER=false
IS_DELETE_DATA=true
IS_SENSOR_TS_ALIGNMENT=true
---------------------------------------------------------------
main measurements:
Create schema cost 0.04 second
Test elapsed time (not include schema creation): 0.63 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           5000                500000              0                   0                   796890.43           
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           0.39        0.10        0.12        0.13        0.15        0.20        0.44        0.67        2.33        9.94        124.64      397.86      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 6.9. Single-point query mode for Verificaiton (single database, external data set)

Before running this mode, you need to use the correctness write mode to write data to the database.

In order to verify the correctness of the data set writing, you can use this mode to query the data set written to the database. Currently this mode only supports IoTDB v0.12

### 6.9.1. Configure

For this, you need to modify the following configuration in ```config.properties```:

```
BENCHMARK_WORK_MODE=verificationQueryMode
# Data set storage location
FILE_PATH=data/test
# The number of batches contained in each data file
BIG_BATCH_SIZE=100
```

> Note:
> 1. The FILE_PATH folder should be the data set generated using the generated data mode.
> 2. When running this mode, other parameters should be **consistent** with the description in info.txt.

### 6.9.2. Start

Before running the test, you need to open the IoTDB service on port 6667.

Then, you can go to `/iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.9.3. Execute

Now after launching the test, you will see testing information rolling like following: 

```
...
11:31:32.595 [pool-2-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-1 50.60% workload is done.
11:31:32.595 [pool-6-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-3 51.30% workload is done.
11:31:32.595 [pool-4-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-2 51.60% workload is done.
11:31:32.595 [pool-10-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-5 50.90% workload is done.
11:31:32.595 [pool-8-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-4 51.40% workload is done.
...
```

When test is done, the last testing information will be like the following:

```
11:31:37.070 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.BaseMode - All clients finished.
----------------------Main Configurations----------------------
CREATE_SCHEMA=true
START_TIME=2018-9-20T00:00:00+08:00
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
BATCH_SIZE_PER_WRITE=10
IS_CLIENT_BIND=true
LOOP=1000
IS_OUT_OF_ORDER=false
IS_REGULAR_FREQUENCY=true
GROUP_NUMBER=20
IS_RECENT_QUERY=true
QUERY_INTERVAL=250000
SENSOR_NUMBER=10
RESULT_PRECISION=0.1%
POINT_STEP=5000
CLIENT_NUMBER=5
SG_STRATEGY=mod
REAL_INSERT_RATE=1.0
OUT_OF_ORDER_MODE=POISSON
DBConfig=
  DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
  HOST=[127.0.0.1]
  PORT=[6667]
  USERNAME=root
  PASSWORD=root
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=false
BENCHMARK_WORK_MODE=verificationQueryMode
OP_INTERVAL=0
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1
DEVICE_NUMBER=5
OUT_OF_ORDER_RATIO=0.5
BENCHMARK_CLUSTER=false
IS_DELETE_DATA=true
IS_SENSOR_TS_ALIGNMENT=true
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 10.49 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
VERIFICATION_QUERY  5000                500000              0                   0                   47644.92            
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
VERIFICATION_QUERY  10.25       3.47        6.49        7.90        9.17        10.53       13.41       16.72       34.40       53.20       245.16      10280.50    
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 6.10. Dual database mode

In order to more conveniently and quickly complete the correctness verification, iotdb-benchmark also supports dual database mode.

1. For all the test scenarios mentioned above, unless otherwise specified, dual databases are supported. Please **start the test** in the `verification` project.
2. For the relevant test scenarios for correctness verification below, all must be run in dual database mode, and currently **only supports** iotdb-0.12 and timescaledb.

In order to complete the dual database configuration, you need to make the following modifications to `config.properties`:

```
# Double-write mode only supports comparison between different databases, and does not support double-write in different versions of the same database
IS_DOUBLE_WRITE=true
# Another written database, the current format is {name}{-version}{-insert mode} (note-number). For all reference values, please refer to the README file
ANOTHER_DB_SWITCH=TimescaleDB
# The host of another written database
ANOTHER_HOST=127.0.0.1
# The port of another database to write to
ANOTHER_PORT=5432
# The username of another database to write to
ANOTHER_USERNAME=postgres
# The password of another database to be written, if it is multiple databases, it must be consistent
ANOTHER_PASSWORD=postgres
# The name of another written database
ANOTHER_DB_NAME=postgres
# Another Token used for database authentication is currently limited to InfluxDB 2.0
ANOTHER_TOKEN=token
# Whether to compare the query result sets in the two databases
IS_COMPARISON=false
# Whether to compare the point-to-point data between the two databases
IS_POINT_COMPARISON=false
```

## 6.11. Writing in conventional test mode (dual database)

In order to verify the correctness below, you first need to write the data to two databases.

### 6.11.1. Configure

Complete the dual database configuration in `config.properties` as described in the dual database mode

In addition, please modify the following configuration in `config.properties`:

```
BENCHMARK_WORK_MODE=testWithDefaultPath
DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
HOST=127.0.0.1
PORT=6667
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
GROUP_NUMBER=20
DEVICE_NUMBER=20
SENSOR_NUMBER=300
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=1
POINT_STEP=5000
LOOP=1000
```

### 6.11.2. Start

Before starting the test, you need to start the IoTDB service on port 6667 of the machine, and start the TimescaleDB service on port 5432

Then you enter `iotdb-benchmark/verfication/target/verification-0.0.1` and run the following command to start Benchmark (currently only execute the following script in Unix/OS X system):

```sh
> ./benchmark.sh
```

### 6.11.3. Excute

After writing data is started, you can see the scrolling execution information, some of which are as follows:

```
...
11:49:14.691 [pool-55-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client-pool-1-thread-18 89.90% workload is done.
11:49:14.691 [pool-16-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client-pool-1-thread-5 90.40% workload is done.
11:49:14.692 [pool-58-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client-pool-1-thread-19 88.50% workload is done.
11:49:14.692 [pool-19-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client-pool-1-thread-6 90.40% workload is done.
11:49:14.691 [pool-52-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client-pool-1-thread-17 88.10% workload is done.
...
```

When the test is over, the information written to the data set will be displayed at the end, as shown below:

```
11:49:17.634 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.BaseMode - All clients finished.
----------------------Main Configurations----------------------
CREATE_SCHEMA=true
START_TIME=2018-9-20T00:00:00+08:00
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
BATCH_SIZE_PER_WRITE=1
IS_CLIENT_BIND=true
LOOP=1000
IS_OUT_OF_ORDER=false
IS_REGULAR_FREQUENCY=true
GROUP_NUMBER=20
IS_COMPASSION=false
IS_RECENT_QUERY=true
QUERY_INTERVAL=250000
SENSOR_NUMBER=300
RESULT_PRECISION=0.1%
POINT_STEP=5000
CLIENT_NUMBER=20
SG_STRATEGY=mod
REAL_INSERT_RATE=1.0
ANOTHER DBConfig=
  DB_SWITCH=TimescaleDB
  HOST=[127.0.0.1]
  PORT=[5432]
  USERNAME=postgres
  PASSWORD=postgres
  DB_NAME=postgres
  TOKEN=token
OUT_OF_ORDER_MODE=POISSON
DBConfig=
  DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
  HOST=[127.0.0.1]
  PORT=[6667]
  USERNAME=root
  PASSWORD=root
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=true
BENCHMARK_WORK_MODE=testWithDefaultPath
OP_INTERVAL=0
IS_POINT_COMPARISON=false
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
DEVICE_NUMBER=20
OUT_OF_ORDER_RATIO=0.5
BENCHMARK_CLUSTER=false
IS_DELETE_DATA=true
IS_SENSOR_TS_ALIGNMENT=true
---------------------------------------------------------------
main measurements:
Create schema cost 0.06 second
Test elapsed time (not include schema creation): 23.97 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           40000               12000000            0                   0                   500551.60           
PRECISE_POINT       0                   0                   0                   0                   0.00                
TIME_RANGE          0                   0                   0                   0                   0.00                
VALUE_RANGE         0                   0                   0                   0                   0.00                
AGG_RANGE           0                   0                   0                   0                   0.00                
AGG_VALUE           0                   0                   0                   0                   0.00                
AGG_RANGE_VALUE     0                   0                   0                   0                   0.00                
GROUP_BY            0                   0                   0                   0                   0.00                
LATEST_POINT        0                   0                   0                   0                   0.00                
RANGE_QUERY_DESC    0                   0                   0                   0                   0.00                
VALUE_RANGE_QUERY_DESC0                   0                   0                   0                   0.00                
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           11.70       0.34        0.50        0.60        7.75        21.32       27.84       32.53       42.92       65.38       161.62      23740.74    
PRECISE_POINT       0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
TIME_RANGE          0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE         0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_RANGE           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_VALUE           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_RANGE_VALUE     0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
GROUP_BY            0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
LATEST_POINT        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
RANGE_QUERY_DESC    0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE_QUERY_DESC0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 6.12. Single-point query mode for correctness (dual database comparison)

In order to verify the correctness of the database data more efficiently, iotdb-benchmark provides to complete the correctness verification by comparing the data between the two databases.

Note that before performing this test, please use the regular test mode write (dual database) above to complete the database write, it is currently recommended using the JDBC method

### 6.12.1. Configure

As described in the dual database mode, complete the dual database configuration in `config.properties`, modify the following configuration, and start the single-point query for correctness (dual database comparison)

```
# Whether to compare the point-to-point data between the two databases
IS_POINT_COMPARISON=true
```

In addition, please modify the following configuration in `config.properties`:

```
BENCHMARK_WORK_MODE=testWithDefaultPath
DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
HOST=127.0.0.1
PORT=6667
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
GROUP_NUMBER=20
DEVICE_NUMBER=20
SENSOR_NUMBER=300
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=1
POINT_STEP=5000
LOOP=1000
```

### 6.12.2. Start

Before starting the test, you need to start the IoTDB service on port 6667 of the machine, and start the TimescaleDB service on port 5432

Then you enter `iotdb-benchmark/verfication/target/verification-0.0.1` and run the following command to start Benchmark (currently only execute the following script in Unix/OS X system):

```sh
> ./benchmark.sh
```

### 6.12.3. Execute

After writing data is started, you can see the scrolling execution information, some of which are as follows:

```
...
11:53:02.347 [pool-74-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper - pool-1-thread-11 97.90% syntheticClient for d_0 is done.
11:53:02.347 [pool-73-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper - pool-1-thread-12 98.90% syntheticClient for d_0 is done.
11:53:02.354 [pool-76-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper - pool-1-thread-4 100.00% syntheticClient for d_0 is done.
...
```

When the test is over, the information written to the data set will be displayed at the end, as shown below:

```
----------------------Main Configurations----------------------
CREATE_SCHEMA=false
START_TIME=2018-9-20T00:00:00+08:00
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
BATCH_SIZE_PER_WRITE=1
IS_CLIENT_BIND=true
LOOP=1000
IS_OUT_OF_ORDER=false
IS_REGULAR_FREQUENCY=true
GROUP_NUMBER=20
IS_COMPASSION=false
IS_RECENT_QUERY=false
QUERY_INTERVAL=250000
SENSOR_NUMBER=300
RESULT_PRECISION=0.1%
POINT_STEP=5000
CLIENT_NUMBER=20
SG_STRATEGY=mod
REAL_INSERT_RATE=1.0
ANOTHER DBConfig=
  DB_SWITCH=TimescaleDB
  HOST=[127.0.0.1]
  PORT=[5432]
  USERNAME=postgres
  PASSWORD=postgres
  DB_NAME=postgres
  TOKEN=token
OUT_OF_ORDER_MODE=POISSON
DBConfig=
  DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
  HOST=[127.0.0.1]
  PORT=[6667]
  USERNAME=root
  PASSWORD=root
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=true
BENCHMARK_WORK_MODE=testWithDefaultPath
OP_INTERVAL=0
IS_POINT_COMPARISON=true
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
DEVICE_NUMBER=20
OUT_OF_ORDER_RATIO=0.5
BENCHMARK_CLUSTER=false
IS_DELETE_DATA=false
IS_SENSOR_TS_ALIGNMENT=true
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 4.17 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
DEVICE_QUERY        40                  12027960            0                   0                   11054800.40         
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
DEVICE_QUERY        610.61      365.63      383.00      400.94      432.11      838.37      862.54      864.10      866.40      866.31      866.30      1295.73     
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 6.13. Correctness function query mode (dual database comparison)

In order to verify the correctness of the database query more efficiently, iotdb-benchmark provides to complete the correctness verification by comparing the difference of the data query results between the two databases.

Notice:

1. Before performing this test, please use the regular test mode write (dual database) above to complete the database write.
2. The value of LOOP **cannot be too large** to satisfy: LOOP(query) * QUERY_INTERVAL(query) * DEVICE_NUMBER(write) <= LOOP(write) * POINT_STEP(write)

### 6.13.1. Configure

As described in the dual database mode, complete the dual database configuration in `config.properties`, modify the following configuration, and start the single-point query for correctness (dual database comparison)

```
# Whether to compare the query result sets in the two databases
IS_COMPARISON=true
```

In addition, please modify the following configuration in `config.properties` (Note: `LOOP=100`, to avoid query beyond the writing range)

```
BENCHMARK_WORK_MODE=testWithDefaultPath
DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
HOST=127.0.0.1
PORT=6667
OPERATION_PROPORTION=0:1:1:1:1:1:1:1:1:1:1
GROUP_NUMBER=20
DEVICE_NUMBER=20
SENSOR_NUMBER=300
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=1
POINT_STEP=5000
LOOP=100
```

### 6.13.2. Start

Before starting the test, you need to start the IoTDB service on port 6667 of the machine, and start the TimescaleDB service on port 5432

Then you enter `iotdb-benchmark/verfication/target/verification-0.0.1` and run the following command to start Benchmark (currently only execute the following script in Unix/OS X system):

```sh
> ./benchmark.sh
```

### 6.13.3. Execute

After writing data is started, you can see the scrolling execution information, some of which are as follows:

```
...
12:08:21.087 [pool-37-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client-pool-1-thread-12 70.00% workload is done.
12:08:21.087 [pool-55-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client-pool-1-thread-18 68.00% workload is done.
12:08:21.087 [pool-43-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client-pool-1-thread-14 78.00% workload is done.
12:08:21.087 [pool-46-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client-pool-1-thread-15 80.00% workload is done.
...
```

When the test is over, the information written to the data set will be displayed at the end, as shown below:

```
12:08:21.362 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.BaseMode - All clients finished.
----------------------Main Configurations----------------------
CREATE_SCHEMA=false
START_TIME=2018-9-20T00:00:00+08:00
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
BATCH_SIZE_PER_WRITE=1
IS_CLIENT_BIND=true
LOOP=100
IS_OUT_OF_ORDER=false
IS_REGULAR_FREQUENCY=true
GROUP_NUMBER=20
IS_COMPASSION=true
IS_RECENT_QUERY=false
QUERY_INTERVAL=250000
SENSOR_NUMBER=300
RESULT_PRECISION=0.1%
POINT_STEP=5000
CLIENT_NUMBER=20
SG_STRATEGY=mod
REAL_INSERT_RATE=1.0
ANOTHER DBConfig=
  DB_SWITCH=TimescaleDB
  HOST=[127.0.0.1]
  PORT=[5432]
  USERNAME=postgres
  PASSWORD=postgres
  DB_NAME=postgres
  TOKEN=token
OUT_OF_ORDER_MODE=POISSON
DBConfig=
  DB_SWITCH=IoTDB-012-SESSION_BY_TABLET
  HOST=[127.0.0.1]
  PORT=[6667]
  USERNAME=root
  PASSWORD=root
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=true
BENCHMARK_WORK_MODE=testWithDefaultPath
OP_INTERVAL=0
IS_POINT_COMPARISON=false
OPERATION_PROPORTION=0:1:1:1:1:1:1:1:1:1:1
DEVICE_NUMBER=20
OUT_OF_ORDER_RATIO=0.5
BENCHMARK_CLUSTER=false
IS_DELETE_DATA=false
IS_SENSOR_TS_ALIGNMENT=true
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 1.31 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           0                   0                   0                   0                   0.00                
PRECISE_POINT       430                 428                 0                   0                   326.21              
TIME_RANGE          398                 20296               0                   0                   15468.98            
VALUE_RANGE         374                 19072               0                   0                   14536.08            
AGG_RANGE           410                 410                 0                   0                   312.49              
AGG_VALUE           400                 400                 0                   0                   304.87              
AGG_RANGE_VALUE     185                 185                 0                   0                   141.00              
GROUP_BY            368                 4784                0                   0                   3646.22             
LATEST_POINT        438                 438                 0                   0                   333.83              
RANGE_QUERY_DESC    422                 21522               0                   0                   16403.40            
VALUE_RANGE_QUERY_DESC390                 19890               0                   0                   15159.54            
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT       3.68        0.57        0.65        0.73        2.69        5.00        7.62        11.49       19.77       36.61       35.21       125.62      
TIME_RANGE          4.14        0.60        0.69        0.78        3.26        6.30        9.77        11.75       16.65       33.62       31.49       154.34      
VALUE_RANGE         4.53        0.56        0.70        0.77        3.28        6.94        10.63       12.38       19.47       44.93       41.90       164.10      
AGG_RANGE           4.50        0.55        0.66        0.72        3.47        6.87        9.46        11.55       21.93       44.14       40.56       176.79      
AGG_VALUE           11.94       0.70        0.83        0.92        4.92        19.43       30.13       36.91       59.25       121.43      114.04      351.56      
AGG_RANGE_VALUE     8.18        2.80        3.64        4.95        6.60        8.98        13.29       17.09       34.23       49.49       47.12       121.37      
GROUP_BY            4.81        0.56        0.64        0.72        3.16        7.49        11.18       14.29       23.51       27.56       26.93       145.47      
LATEST_POINT        7.31        0.35        0.50        0.59        3.26        6.38        27.74       38.16       49.25       49.31       49.31       207.71      
RANGE_QUERY_DESC    4.67        0.57        0.68        0.77        3.15        6.74        10.19       12.99       30.43       51.53       46.21       148.04      
VALUE_RANGE_QUERY_DESC5.05        0.59        0.71        0.81        3.36        7.08        11.07       16.40       29.38       49.00       44.73       151.20      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

# 7. Test Other Database(Part)

## 7.1. Test InfluxDB v1.x
[Quick Start](influxdb/README.md)

## 7.2. Test InfluxDB v2.0
[Quick Start](influxdb-2.0/README.md)

## 7.3. Test Microsoft SQL Server
[Quick Start](mssqlserver/README.md)

## 7.4. Test QuestDB
[Quick Start](questdb/README.md)

## 7.5. Test SQLite
[Quick Start](sqlite/README.md)

## 7.6. Test Victoriametrics
[Quick Start](victoriametrics/README.md)

## 7.7. Test TimeScaleDB
[Quick Start](timescaledb/README.md)

## 7.8. Test PI Archive

[Quick Start](./pi/README.md)

## 7.9. Test TDenginee
[Quick Start](./tdengine/README.md)

# 8. Further explanation of correctness verification

1. Now verification **only support** IoTDB v0.12 and TimescaleDB
2. [Quick Start](verification/README.md)

# 9. Perform Multiple Tests Automatically

Usually a single test is meaningless unless it is compared with other test results. Therefore we provide a interface to execute multiple tests by one launch.

## 9.1. Configure routine

Each line of this file should be the parameters each test process will change(otherwise it becomes replication test). For example, the 'routine' file is:

```
LOOP=10 DEVICE_NUMBER=100 TEST
LOOP=20 DEVICE_NUMBER=50 TEST
LOOP=50 DEVICE_NUMBER=20 TEST
```

Then it will serially execute 3 test process with LOOP parameter are 10, 20 and 50, respectively.

> NOTE:
You can change multiple parameters in each test with format like 'LOOP=20 DEVICE_NUMBER=10 TEST', unnecessary space is not allowed. The key word 'TEST' means a new test begins. If you change different parameters, the changed parameters will remain in next tests.

## 9.2. Start 

After configuring the file 'routine', you can launch the multi-test task by startup script:

```
> ./rep-benchmark.sh
```

Then the test information will show in terminal. 

> NOTE:
If you close the terminal or lose connection to client machine, the test process will terminate. It is the same to any other cases if the output is transmit to terminal.

Using this interface usually takes a long time, you may want to execute the test process as daemon. In this way, you can just launch the test task as daemon by startup script:

```sh
> ./rep-benchmark.sh > /dev/null 2>&1 &
```

In this case, if you want to know what is going on, you can check the log information by command as following:

```sh
> cd ./logs
> tail -f log_info.log
```

# 10. Developer Guidelines
1. All the interfaces of IoTDB-Benchmark are in the core module.
2. The realization of all database tests of IoTDB-Benchmark are in each maven sub-project.
3. If you want to use an editor such as IDEA to run Benchmark:
    1. You can find TestEntrance in the test file directory under each maven subproject, and run the corresponding test.
    2. Taking IoTDB 0.12 as an example, you can run `iotdb-0.12/src/main/test/cn/edu/tsinghua/iotdb/benchmark/TestEntrance`

# 11. Related Article
Benchmark Time Series Database with IoTDB-Benchmark for IoT Scenarios

Arxiv: https://arxiv.org/abs/1901.08304

