# 1. iot-benchmark
![](https://img.shields.io/badge/platform-MacOS%20%7C%20Linux%20%7C%20Windows-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)

You can also read [中文版本](README-cn.md).

# 2. Table of Contents

- [1. iot-benchmark](#1-iot-benchmark)
- [2. Table of Contents](#2-table-of-contents)
- [3. Overview](#3-overview)
- [4. Main Features](#4-main-features)
- [5. Usage of iot-benchmark](#5-usage-of-iot-benchmark)
  - [5.1. Prerequisites of iot-benchmark](#51-prerequisites-of-iot-benchmark)
  - [5.2. Working modes of iot-benchmark](#52-working-modes-of-iot-benchmark)
  - [5.3. Build of iot-benchmark](#53-build-of-iot-benchmark)
- [6. Explanation of different operating modes of iot-benchmark](#6-explanation-of-different-operating-modes-of-iot-benchmark)
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

iot-benchmark is a tool for benchmarking IoTDB against other databases and time series solutions.

Databases currently supported:

|       Database       | Version  |                       Insert_Mode                        |
| :------------------: | :------: | :------------------------------------------------------: |
|        IoTDB         |   v1.0   | jdbc、sessionByTablet、sessionByRecord、sessionByRecords |
|        IoTDB         |  v0.13   | jdbc、sessionByTablet、sessionByRecord、sessionByRecords |
|        IoTDB         |  v0.12   | jdbc、sessionByTablet、sessionByRecord、sessionByRecords |
|       InfluxDB       |   v1.x   |                           SDK                            |
|       InfluxDB       |   v2.0   |                           SDK                            |
|       QuestDB        |  v6.0.7  |                           jdbc                           |
| Microsoft SQL Server | 2016 SP2 |                           jdbc                           |
|   VictoriaMetrics    | v1.64.0  |                       Http Request                       |
|        SQLite        |    --    |                           jdbc                           |
|       OpenTSDB       |  2.4.1   |                       Http Request                       |
|       KairosDB       |    --    |                       Http Request                       |
|     TimescaleDB      |    --    |                           jdbc                           |
|     TimescaleDB      | Cluster  |                           jdbc                           |
|       TDengine       | 2.2.0.2  |                           jdbc                           |
|       TDengine       |  3.0.1   |                           jdbc                           |
|      PI Archive      |   2016   |                           jdbc                           |

# 4. Main Features

iot-benchmark's features are as following:

1. Easy to use: iot-benchmark is a tool combined multiple testing functions so users do not need to switch different tools. 
2. Various data ingestion and testing mode:
   1. Generate periodic time series data according to the configuration and insert and query directly.
   2. Write the generated data to the corresponding location on the disk.
   3. Load data from the generated data set generated in the disk, and write and query.
   4. Perform correctness verification tests on data and query results respectively.
3. Testing report and result: Supporting storing testing information and results for further query or analysis.
4. Visualize test results: Integration with Tableau to visualize the test result.
5. We recommend using MacOs or Linux systems. This article takes MacOS and Linux systems as examples. If you use Windows systems, please use the `benchmark.bat` script in the `conf` folder to start the benchmark.

# 5. Usage of iot-benchmark

## 5.1. Prerequisites of iot-benchmark

To use iot-benchmark, you need to have:

1. Java 8
2. Maven: It is not recommended to use the mirror.
3. The appropriate version of the database
   1. Apache IoTDB >= v0.12 ([Get it!](https://github.com/apache/iotdb))
   2. His corresponding version of the database
4. ServerMode and CSV recording modes can only be used in Linux systems to record relevant system information during the test.

## 5.2. Working modes of iot-benchmark
|           The name of mode            |  BENCHMARK_WORK_MODE  | The content of mode                                                                                                                              |
| :-----------------------------------: | :-------------------: | :----------------------------------------------------------------------------------------------------------------------------------------------- |
|        Conventional test mode         |  testWithDefaultPath  | Supports mixed loads of multiple read and write operations                                                                                       |
|          Generate data mode           |   generateDataMode    | Benchmark generates the data set to the FILE_PATH path                                                                                           |
|      Write mode of verification       | verificationWriteMode | Load the data set from the FILE_PATH path for writing, currently supports IoTDB v0.12 and v0.13                                                  |
|      Query mode of verification       | verificationQueryMode | Load the data set from the FILE_PATH path and compare it with the database. Currently, IoTDB v0.12 and v0.13 is supported                        |
| Server resource usage monitoring mode |      serverMODE       | Server resource usage monitoring mode (run in this mode is started by the ser-benchmark.sh script, no need to manually configure this parameter) |

## 5.3. Build of iot-benchmark

You can build iot-benchmark using Maven, running following command in the **root** of project:

```
mvn clean package -Dmaven.test.skip=true
```

This will compile all versions of IoTDB and other database benchmark. if you want to compile a specific database, go to the package and run above command.

After, for example, you can go to `/iot-benchmark/iotdb-0.13/target/iot-benchmark-iotdb-0.13`, and run `./benchmark.sh` to start iot-benchmark.

The default configuration file is stored under `iot-benchmark/iotdb-0.13/target/iot-benchmark-iotdb-0.13/conf`, you can edit `config.properties` to complete the configuration, please **note that you need Adjust the DB_SWITCH parameter in the configuration file to the database you need to be tested**. The corresponding relationship and possible values are as follows:

|       Database       | Version  | Corresponding Sub-project |                                                  DB_SWITCH                                                   |
| :------------------: | :------: | :-----------------------: | :----------------------------------------------------------------------------------------------------------: |
|        IoTDB         |   0.13   |         iotdb-1.0         | IoTDB-100-JDBC<br>IoTDB-100-SESSION_BY_TABLET<br>IoTDB-100-SESSION_BY_RECORD<br>IoTDB-100-SESSION_BY_RECORDS |
|        IoTDB         |   1.0    |        iotdb-0.13         | IoTDB-013-JDBC<br>IoTDB-013-SESSION_BY_TABLET<br>IoTDB-013-SESSION_BY_RECORD<br>IoTDB-013-SESSION_BY_RECORDS |
|        IoTDB         |   0.12   |        iotdb-0.12         | IoTDB-012-JDBC<br>IoTDB-012-SESSION_BY_TABLET<br>IoTDB-012-SESSION_BY_RECORD<br>IoTDB-012-SESSION_BY_RECORDS |
|       InfluxDB       |   v1.x   |         influxdb          |                                                   InfluxDB                                                   |
|       InfluxDB       |   v2.0   |       influxdb-2.0        |                                                 InfluxDB-2.0                                                 |
|       QuestDB        |  v6.0.7  |          questdb          |                                                   QuestDB                                                    |
| Microsoft SQL Server | 2016 SP2 |        mssqlserver        |                                                 MSSQLSERVER                                                  |
|   VictoriaMetrics    | v1.64.0  |      victoriametrics      |                                               VictoriaMetrics                                                |
|     TimescaleDB      |    --    |        timescaledb        |                                                 TimescaleDB                                                  |
|     TimescaleDB      | Cluster  |    timescaledb-cluster    |                                             TimescaleDB-Cluster                                              |
|        SQLite        |    --    |          sqlite           |                                                    SQLite                                                    |
|       OpenTSDB       |  2.4.1   |         opentsdb          |                                                   OpenTSDB                                                   |
|       KairosDB       |    --    |         kairosdb          |                                                   KairosDB                                                   |
|       TDengine       | 2.2.0.2  |         TDengine          |                                                   TDengine                                                   |
|       TDengine       |  3.0.1   |         TDengine          |                                                  TDengine-3                                                  |
|      PI Archive      |   2016   |         PIArchive         |                                                  PIArchive                                                   |

# 6. Explanation of different operating modes of iot-benchmark
All of the following tests were performed in the following environment:

```
CPU: I7-11700
Memory: 32G DDR4
System disk: 512G SSD (INTEL SSDPEKNU512GZ)
Data disk: 2T HDD (WDC WD40EZAZ-00SF3B0)
```

## 6.1. Write of Conventional test mode(Single database)

This short guide will walk you through the basic process of using iot-benchmark.

### 6.1.1. Configure

Before starting any new test case, you need to config the configuration files ```config.properties``` first. For your convenience, we have already set the default config for the following demonstration.

Suppose you are going to test data ingestion performance of IoTDB. You have installed IoTDB dependencies and launched a IoTDB server with default settings. The IP of the server is 127.0.0.1. Suppose the workload parameters are:

```
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
| Measurement/StorageGroup | tag/device | field/sensor | concurrency | batch size | point interval(ms) |  loop  |
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
|            10            |    50      |      500     |     20      |    100     |        200         |  10000 |
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
```

Notice: Under this configuration, the total timeseries number is ```deivce * sensor = 25,000```, the number of data points
in each timeseries is ```batch size * loop = 20,000```, total data points is ```deivce * sensor * batch size * loop = 500,000,000```.
According to 16bytes of each data point, the total raw data size is 8G.

edit the corresponding parameters in the ```config.properties``` file as following:

```properties
HOST=127.0.0.1
PORT=6667
DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
BENCHMARK_WORK_MODE=testWithDefaultPath
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
GROUP_NUMBER=10
DEVICE_NUMBER=50
SENSOR_NUMBER=500
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=100
POINT_STEP=200
LOOP=10000
```

Currently, you can edit other configs, more config in [config.properties](configuration/conf/config.properties)

### 6.1.2. Start(Without Server System Information Recording)

Before running the test, you need to open the IoTDB service on port 6667.

Then, you can go to `/iot-benchmark/iotdb-0.13/target/iot-benchmark-iotdb-0.13`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.1.3. Execute 

Now after launching the test, you will see testing information rolling like following: 

```
...
2022-05-08 14:26:36,478 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-4 17.10% workload is done. 
2022-05-08 14:26:41,479 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-13 56.59% workload is done. 
2022-05-08 14:26:41,479 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-2 18.01% workload is done. 
2022-05-08 14:26:41,480 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-14 54.01% workload is done. 
...
```

When test is done, the last output of the test information will be like following: 

```
...
2022-05-08 14:40:54,243 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=false
DBConfig=
  DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
  HOST=[127.0.0.1]
########### Data Mode ###########
GROUP_NUMBER=10
DEVICE_NUMBER=50
REAL_INSERT_RATE=1.0
SENSOR_NUMBER=500
IS_SENSOR_TS_ALIGNMENT=true
IS_OUT_OF_ORDER=false
OUT_OF_ORDER_RATIO=0.5
########### Data Amount ###########
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
CLIENT_NUMBER=20
LOOP=10000
BATCH_SIZE_PER_WRITE=100
START_TIME=2022-01-01T00:00:00+08:00
POINT_STEP=200
OP_MIN_INTERVAL=0
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
ENCODINGS=PLAIN/PLAIN/PLAIN/PLAIN/PLAIN/PLAIN
COMPRESSOR=SNAPPY
########### Other Param ###########
IS_DELETE_DATA=true
CREATE_SCHEMA=true
BENCHMARK_CLUSTER=false

---------------------------------------------------------------
main measurements:
Create schema cost 0.30 second
Test elapsed time (not include schema creation): 1238.79 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)      
INGESTION                500000                   25000000000              0                        0                        20180954.09              
PRECISE_POINT            0                        0                        0                        0                        0.00                     
TIME_RANGE               0                        0                        0                        0                        0.00                     
VALUE_RANGE              0                        0                        0                        0                        0.00                     
AGG_RANGE                0                        0                        0                        0                        0.00                     
AGG_VALUE                0                        0                        0                        0                        0.00                     
AGG_RANGE_VALUE          0                        0                        0                        0                        0.00                     
GROUP_BY                 0                        0                        0                        0                        0.00                     
LATEST_POINT             0                        0                        0                        0                        0.00                     
RANGE_QUERY_DESC         0                        0                        0                        0                        0.00                     
VALUE_RANGE_QUERY_DESC   0                        0                        0                        0                        0.00                     
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION                37.78       1.67        2.02        2.29        2.86        4.14        5.62        7.43        759.69      5799.89     8309.40     1227561.44  
PRECISE_POINT            0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
TIME_RANGE               0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE              0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_RANGE                0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_VALUE                0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_RANGE_VALUE          0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
GROUP_BY                 0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
LATEST_POINT             0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
RANGE_QUERY_DESC         0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE_QUERY_DESC   0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
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
  + <a href = "https://y8dp9fjm8f.feishu.cn/file/boxcn6dA7ikCNswUwygRbdOu3wp">Detailed parameter description</a>
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
DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
BENCHMARK_WORK_MODE=testWithDefaultPath
OPERATION_PROPORTION=0:1:1:1:1:1:1:1:1:1:1
GROUP_NUMBER=10
DEVICE_NUMBER=50
SENSOR_NUMBER=500
CLIENT_NUMBER=20
POINT_STEP=200
LOOP=10000

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

Then, you can go to `/iot-benchmark/iotdb-0.13/target/iot-benchmark-iotdb-0.13`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.2.3. Execute 

Now after launching the test, you will see testing information rolling like following: 

```
...
2022-05-08 14:55:37,228 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-14 93.37% workload is done. 
2022-05-08 14:55:37,228 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-17 94.40% workload is done. 
2022-05-08 14:55:37,228 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-8 99.43% workload is done. 
2022-05-08 14:55:37,228 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-18 97.40% workload is done. 
...
```

When test is done, the last testing information will be like the following: 

```
2022-05-08 14:55:47,915 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=false
DBConfig=
  DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
  HOST=[127.0.0.1]
########### Data Mode ###########
GROUP_NUMBER=10
DEVICE_NUMBER=50
REAL_INSERT_RATE=1.0
SENSOR_NUMBER=500
IS_SENSOR_TS_ALIGNMENT=true
IS_OUT_OF_ORDER=false
OUT_OF_ORDER_RATIO=0.5
########### Data Amount ###########
OPERATION_PROPORTION=0:1:1:1:1:1:1:1:1:1:1
CLIENT_NUMBER=20
LOOP=10000
BATCH_SIZE_PER_WRITE=100
START_TIME=2022-01-01T00:00:00+08:00
POINT_STEP=200
OP_MIN_INTERVAL=0
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
ENCODINGS=PLAIN/PLAIN/PLAIN/PLAIN/PLAIN/PLAIN
COMPRESSOR=SNAPPY
########### Query Param ###########
QUERY_DEVICE_NUM=1
QUERY_SENSOR_NUM=1
QUERY_INTERVAL=250000
STEP_SIZE=1
IS_RECENT_QUERY=false
########### Other Param ###########
IS_DELETE_DATA=false
CREATE_SCHEMA=false
BENCHMARK_CLUSTER=false

---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 196.72 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)      
INGESTION                0                        0                        0                        0                        0.00                     
PRECISE_POINT            20005                    19964                    0                        0                        101.49                   
TIME_RANGE               19825                    24800244                 0                        0                        126070.25                
VALUE_RANGE              19885                    24875263                 0                        0                        126451.61                
AGG_RANGE                20204                    20204                    0                        0                        102.71                   
AGG_VALUE                20110                    20110                    0                        0                        102.23                   
AGG_RANGE_VALUE          19799                    19799                    0                        0                        100.65                   
GROUP_BY                 20243                    263159                   0                        0                        1337.75                  
LATEST_POINT             19953                    19953                    0                        0                        101.43                   
RANGE_QUERY_DESC         20090                    25131637                 0                        0                        127754.87                
VALUE_RANGE_QUERY_DESC   19886                    24876158                 0                        0                        126456.16                
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION                0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT            4.53        0.21        1.39        1.94        2.77        4.17        8.77        15.91       32.66       54.53       92.02       5219.85     
TIME_RANGE               5.43        0.41        1.93        2.55        3.50        5.10        10.88       17.57       34.94       57.50       185.37      6210.07     
VALUE_RANGE              5.44        0.50        2.05        2.67        3.62        5.12        10.10       17.49       34.12       60.48       110.57      5963.43     
AGG_RANGE                4.53        0.30        1.44        2.00        2.88        4.31        8.35        15.23       31.52       56.92       157.49      5016.19     
AGG_VALUE                147.78      50.81       105.52      141.41      151.14      161.50      173.46      180.87      208.43      392.72      550.38      151938.36   
AGG_RANGE_VALUE          4.82        0.34        1.70        2.27        3.19        4.61        9.10        15.62       31.61       54.62       102.28      5159.73     
GROUP_BY                 4.49        0.29        1.37        1.92        2.77        4.18        8.49        15.65       32.98       53.85       108.91      5020.44     
LATEST_POINT             3.41        0.10        0.66        1.16        1.92        3.18        6.26        12.83       29.63       49.73       116.27      3682.06     
RANGE_QUERY_DESC         5.47        0.46        1.91        2.55        3.51        5.11        10.81       18.19       34.86       60.20       157.94      6107.26     
VALUE_RANGE_QUERY_DESC   5.44        0.52        2.05        2.68        3.62        5.13        10.08       17.21       35.33       57.79       157.40      5696.14     
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
DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
BENCHMARK_WORK_MODE=testWithDefaultPath
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1
GROUP_NUMBER=10
DEVICE_NUMBER=50
SENSOR_NUMBER=500
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=100
POINT_STEP=200
LOOP=10000
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

Then you enter `iot-benchmark/iotdb-0.13/target/iot-benchmark-iotdb-0.13` and run the following command to start Benchmark (currently only execute the following script in Unix/OS X system):

```sh
> ./benchmark.sh
```

### 6.3.3. Execute

After the test is started, you can see the rolling test execution information, some of which are as follows:

```
...
2022-05-08 15:00:23,000 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-10 39.63% workload is done. 
2022-05-08 15:00:23,000 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-1 39.26% workload is done. 
2022-05-08 15:00:23,000 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-19 43.91% workload is done. 
2022-05-08 15:00:23,000 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-16 45.84% workload is done. 
...
```

When the test is over, the statistical information of this test will be displayed at last, as shown below:

```
2022-05-08 15:02:03,959 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=false
DBConfig=
  DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
  HOST=[127.0.0.1]
########### Data Mode ###########
GROUP_NUMBER=10
DEVICE_NUMBER=50
REAL_INSERT_RATE=1.0
SENSOR_NUMBER=500
IS_SENSOR_TS_ALIGNMENT=true
IS_OUT_OF_ORDER=false
OUT_OF_ORDER_RATIO=0.5
########### Data Amount ###########
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1
CLIENT_NUMBER=20
LOOP=10000
BATCH_SIZE_PER_WRITE=100
START_TIME=2022-01-01T00:00:00+08:00
POINT_STEP=200
OP_MIN_INTERVAL=0
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
ENCODINGS=PLAIN/PLAIN/PLAIN/PLAIN/PLAIN/PLAIN
COMPRESSOR=SNAPPY
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

---------------------------------------------------------------
main measurements:
Create schema cost 0.27 second
Test elapsed time (not include schema creation): 166.98 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)      
INGESTION                45590                    2279500000               0                        0                        13650927.24              
PRECISE_POINT            18020                    17974                    0                        0                        107.64                   
TIME_RANGE               18186                    22531715                 0                        0                        134932.57                
VALUE_RANGE              17986                    22246569                 0                        0                        133224.96                
AGG_RANGE                18440                    18440                    0                        0                        110.43                   
AGG_VALUE                18258                    18258                    0                        0                        109.34                   
AGG_RANGE_VALUE          18001                    18001                    0                        0                        107.80                   
GROUP_BY                 18445                    239785                   0                        0                        1435.97                  
LATEST_POINT             18172                    18140                    0                        0                        108.63                   
RANGE_QUERY_DESC         18184                    22500153                 0                        0                        134743.56                
VALUE_RANGE_QUERY_DESC   18093                    22394469                 0                        0                        134110.67                
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION                17.15       1.61        2.18        2.98        4.42        6.00        14.66       108.61      245.55      804.68      1557.66     54020.31    
PRECISE_POINT            12.34       0.11        0.29        0.43        0.98        2.73        6.85        92.59       212.27      565.18      1552.49     13680.36    
TIME_RANGE               13.88       0.28        0.56        0.77        1.46        3.62        8.88        101.47      244.89      649.07      1552.78     14887.20    
VALUE_RANGE              13.46       0.36        0.64        0.86        1.42        3.29        7.95        99.00       216.31      634.16      1457.81     14560.73    
AGG_RANGE                13.01       0.14        0.35        0.50        0.96        2.52        6.58        101.23      226.76      586.09      1549.94     16023.63    
AGG_VALUE                20.40       0.32        2.02        3.56        5.69        9.03        28.50       121.06      254.31      804.12      1486.73     21477.54    
AGG_RANGE_VALUE          13.30       0.23        0.47        0.65        1.16        3.00        7.66        102.09      213.14      606.75      1476.82     17331.58    
GROUP_BY                 13.06       0.15        0.31        0.45        0.91        2.42        6.27        99.64       230.63      625.32      1549.57     14443.18    
LATEST_POINT             1.16        0.06        0.10        0.14        0.23        0.80        1.81        2.92        6.43        127.80      1162.76     2269.69     
RANGE_QUERY_DESC         14.30       0.27        0.56        0.78        1.54        3.68        9.23        106.67      235.38      617.13      1472.53     15579.69    
VALUE_RANGE_QUERY_DESC   13.19       0.37        0.65        0.86        1.46        3.43        8.13        96.87       209.36      614.02      1553.93     15276.43    
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
DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
BENCHMARK_WORK_MODE=testWithDefaultPath
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1
GROUP_NUMBER=10
DEVICE_NUMBER=50
SENSOR_NUMBER=500
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=100
POINT_STEP=200
LOOP=10000
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

Then you enter `iot-benchmark/iotdb-0.13/target/iot-benchmark-iotdb-0.13` and run the following command to start Benchmark (currently only execute the following script in Unix/OS X system):

```sh
> ./benchmark.sh
```

### 6.4.3. Execute

After the test is started, you can see the rolling test execution information, some of which are as follows:

```
...
2022-05-08 15:06:15,298 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-6 88.36% workload is done. 
2022-05-08 15:06:15,298 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-2 88.95% workload is done. 
2022-05-08 15:06:20,298 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-1 97.39% workload is done. 
2022-05-08 15:06:20,298 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-8 95.43% workload is done. 
...
```

When the test is over, the statistical information of this test will be displayed at last, as shown below:

```
2022-05-08 15:06:34,593 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=false
DBConfig=
  DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
  HOST=[127.0.0.1]
########### Data Mode ###########
GROUP_NUMBER=10
DEVICE_NUMBER=50
REAL_INSERT_RATE=1.0
SENSOR_NUMBER=500
IS_SENSOR_TS_ALIGNMENT=true
IS_OUT_OF_ORDER=false
OUT_OF_ORDER_RATIO=0.5
########### Data Amount ###########
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1
CLIENT_NUMBER=20
LOOP=10000
BATCH_SIZE_PER_WRITE=100
START_TIME=2022-01-01T00:00:00+08:00
POINT_STEP=200
OP_MIN_INTERVAL=0
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
ENCODINGS=PLAIN/PLAIN/PLAIN/PLAIN/PLAIN/PLAIN
COMPRESSOR=SNAPPY
########### Query Param ###########
QUERY_DEVICE_NUM=1
QUERY_SENSOR_NUM=1
QUERY_INTERVAL=250000
STEP_SIZE=1
IS_RECENT_QUERY=true
########### Other Param ###########
IS_DELETE_DATA=true
CREATE_SCHEMA=true
BENCHMARK_CLUSTER=false

---------------------------------------------------------------
main measurements:
Create schema cost 0.27 second
Test elapsed time (not include schema creation): 170.32 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)      
INGESTION                45590                    2279500000               0                        0                        13383765.84              
PRECISE_POINT            18020                    8127                     0                        0                        47.72                    
TIME_RANGE               18186                    12021212                 0                        0                        70580.87                 
VALUE_RANGE              17986                    11960816                 0                        0                        70226.26                 
AGG_RANGE                18440                    18440                    0                        0                        108.27                   
AGG_VALUE                18258                    18258                    0                        0                        107.20                   
AGG_RANGE_VALUE          18001                    18001                    0                        0                        105.69                   
GROUP_BY                 18445                    239785                   0                        0                        1407.86                  
LATEST_POINT             18172                    18141                    0                        0                        106.51                   
RANGE_QUERY_DESC         18184                    12016971                 0                        0                        70555.97                 
VALUE_RANGE_QUERY_DESC   18093                    11909073                 0                        0                        69922.46                 
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION                17.74       1.55        2.07        2.82        4.33        5.86        13.95       111.73      260.45      1044.07     1346.06     57226.89    
PRECISE_POINT            13.78       0.08        0.25        0.40        0.82        2.25        6.03        102.67      244.68      983.71      1330.14     17597.87    
TIME_RANGE               13.25       0.08        0.30        0.56        1.13        2.73        7.08        98.43       220.55      793.19      1327.96     15824.01    
VALUE_RANGE              13.40       0.08        0.31        0.61        1.18        2.63        6.96        96.68       250.09      817.78      1319.02     15229.06    
AGG_RANGE                12.29       0.09        0.31        0.50        1.00        2.44        6.25        95.83       204.21      620.53      1169.73     14457.33    
AGG_VALUE                20.69       0.33        2.04        3.51        5.57        8.69        25.15       116.23      275.78      1062.39     1287.66     22518.77    
AGG_RANGE_VALUE          14.10       0.11        0.44        0.70        1.31        3.00        7.69        105.53      224.66      887.40      1344.02     15906.44    
GROUP_BY                 13.11       0.10        0.32        0.48        0.96        2.32        6.15        100.94      239.69      651.59      1329.18     15165.48    
LATEST_POINT             1.16        0.06        0.10        0.14        0.24        0.78        1.77        2.91        7.33        122.85      417.34      1664.48     
RANGE_QUERY_DESC         13.06       0.09        0.31        0.57        1.13        2.81        7.47        98.79       216.61      624.26      1328.28     14898.96    
VALUE_RANGE_QUERY_DESC   13.08       0.08        0.31        0.61        1.16        2.63        7.27        100.01      212.43      710.30      1329.37     14761.17    
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 6.5. Use system records of conventional test mode (single database)

IoTDB Benchmark allows you to use a database to store system data during the test, and currently supports the use of CSV records.

### 6.5.1. Configure

Suppose your IoTDB server IP is 192.168.130.9 and your test client server which installed iot-benchmark has authorized ssh access to the IoTDB server.
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

Then, you can go to `/iot-benchmark/iotdb-0.13/target/iot-benchmark-iotdb-0.13`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

The test results will be saved in the path ```data``` in CSV format

## 6.6. Persistence of the test process in conventional test mode (single database)

For subsequent analysis, iot-benchmark can store test information in the database (if you don't want to store test data, then set ```TEST_DATA_PERSISTENCE=None```)

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

In order to generate a data set that can be reused, iot-benchmark provides a data set generation mode, and the data set is generated to FILE_PATH for subsequent use in the correctness write mode and correctness query mode.

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

Then, you can go to `/iot-benchmark/iotdb-0.13/target/iot-benchmark-iotdb-0.13`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.7.3. Execute

Now after launching the test, you will see testing information rolling. When test is done, the last testing information will be like the following: 

```
2022-05-08 15:07:19,641 INFO  cn.edu.tsinghua.iot.benchmark.mode.GenerateDataMode:43 - Data Location: data/test 
2022-05-08 15:07:19,641 INFO  cn.edu.tsinghua.iot.benchmark.mode.GenerateDataMode:44 - Schema Location: data/test/schema.txt 
2022-05-08 15:07:19,641 INFO  cn.edu.tsinghua.iot.benchmark.mode.GenerateDataMode:45 - Generate Info Location: data/test/info.txt 
```

> Note:
> 1. The data storage location is under the FILE_PATH folder, and its directory structure is /d_xxx/batch_xxx.txt
> 2. The metadata of the device and sensor is stored in FILE_PATH/schema.txt
> 3. The relevant information of the data set is stored in FILE_PATH/info.txt

The following is an example of info.txt:

```
LOOP=10000
BIG_BATCH_SIZE=100
FIRST_DEVICE_INDEX=0
POINT_STEP=200
TIMESTAMP_PRECISION='ms'
STRING_LENGTH=2
DOUBLE_LENGTH=2
INSERT_DATATYPE_PROPORTION='1:1:1:1:1:1'
DEVICE_NUMBER=5
REAL_INSERT_RATE=1.0
SENSOR_NUMBER=10
IS_SENSOR_TS_ALIGNMENT=true
TS_ALIGNMENT_RATIO=1.0
DATA_SEED=666
SG_STRATEGY='mod'
GROUP_NUMBER=10
BATCH_SIZE_PER_WRITE=10
START_TIME='2022-01-01T00:00:00+08:00'
IS_COPY_MODE=false'
IS_ADD_ANOMALY=false
ANOMALY_RATE=0.2
ANOMALY_TIMES=2'
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
WORKLOAD_BUFFER_SIZE=100
SENSORS=[s_0, s_1, s_2, s_3, s_4, s_5, s_6, s_7, s_8, s_9]
```

## 6.8. Write mode for Verificaiton (single database, external data set)

In order to verify the correctness of the data set writing, you can use this mode to write the data set generated in the generated data mode. Currently this mode only supports IoTDB v0.12 , v0.13 and influxdb v1.x

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
FILE_PATH=data/test
BIG_BATCH_SIZE=100
CLIENT_NUMBER=1
BATCH_SIZE_PER_WRITE=100
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

Then, you can go to `/iot-benchmark/iotdb-0.13/target/iot-benchmark-iotdb-0.13`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.8.3. Execute
Now after launching the test, you will see testing information rolling like following:

```
...
2022-05-08 15:08:31,735 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-4 9.86% workload is done. 
2022-05-08 15:08:36,735 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-4 98.24% workload is done. 
2022-05-08 15:08:36,735 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-5 97.08% workload is done. 
2022-05-08 15:08:36,735 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-3 96.54% workload is done. 
...
```

When test is done, the last testing information will be like the following:

```
2022-05-08 15:08:38,751 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=verificationWriteMode
########### Database Connection Information ###########
DOUBLE_WRITE=false
DBConfig=
  DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
  HOST=[127.0.0.1]
########### Data Mode ###########
GROUP_NUMBER=10
DEVICE_NUMBER=5
REAL_INSERT_RATE=1.0
SENSOR_NUMBER=10
IS_SENSOR_TS_ALIGNMENT=true
IS_OUT_OF_ORDER=false
OUT_OF_ORDER_RATIO=0.5
########### Data Amount ###########
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
CLIENT_NUMBER=5
LOOP=10000
BATCH_SIZE_PER_WRITE=10
START_TIME=2022-01-01T00:00:00+08:00
POINT_STEP=200
OP_MIN_INTERVAL=0
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
ENCODINGS=PLAIN/PLAIN/PLAIN/PLAIN/PLAIN/PLAIN
COMPRESSOR=SNAPPY
########### Other Param ###########
IS_DELETE_DATA=true
CREATE_SCHEMA=true
BENCHMARK_CLUSTER=false

---------------------------------------------------------------
main measurements:
Create schema cost 0.03 second
Test elapsed time (not include schema creation): 8.02 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)      
INGESTION                50000                    5000000                  0                        0                        623480.20                
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION                0.52        0.02        0.02        0.03        0.03        0.05        0.09        0.13        0.65        145.45      248.96      5766.33     
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 6.9. Single-point query mode for Verificaiton (single database, external data set)

Before running this mode, you need to use the correctness write mode to write data to the database.

In order to verify the correctness of the data set writing, you can use this mode to query the data set written to the database. Currently this mode only supports IoTDB v0.12 , v0.13 and influxdb v1.x

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

Then, you can go to `/iot-benchmark/iotdb-0.13/target/iot-benchmark-iotdb-0.13`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.9.3. Execute

Now after launching the test, you will see testing information rolling like following: 

```
...
2022-05-08 15:09:38,358 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-3 11.15% workload is done. 
2022-05-08 15:09:38,358 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-4 11.16% workload is done. 
2022-05-08 15:09:38,358 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-1 11.32% workload is done. 
2022-05-08 15:09:43,358 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-4 14.92% workload is done. 
...
```

When test is done, the last testing information will be like the following:

```
2022-05-08 15:11:50,033 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=verificationQueryMode
########### Database Connection Information ###########
DOUBLE_WRITE=false
DBConfig=
  DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
  HOST=[127.0.0.1]
########### Data Mode ###########
GROUP_NUMBER=10
DEVICE_NUMBER=5
REAL_INSERT_RATE=1.0
SENSOR_NUMBER=10
IS_SENSOR_TS_ALIGNMENT=true
IS_OUT_OF_ORDER=false
OUT_OF_ORDER_RATIO=0.5
########### Data Amount ###########
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
CLIENT_NUMBER=5
LOOP=10000
BATCH_SIZE_PER_WRITE=10
START_TIME=2022-01-01T00:00:00+08:00
POINT_STEP=200
OP_MIN_INTERVAL=0
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
ENCODINGS=PLAIN/PLAIN/PLAIN/PLAIN/PLAIN/PLAIN
COMPRESSOR=SNAPPY
########### Other Param ###########
IS_DELETE_DATA=true
CREATE_SCHEMA=true
BENCHMARK_CLUSTER=false

---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 147.72 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)      
VERIFICATION_QUERY       50000                    5000000                  0                        0                        33848.37                 
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
VERIFICATION_QUERY       14.48       0.96        11.40       12.67       14.43       16.36       18.13       19.97       24.74       29.68       138.41      145016.52   
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 6.10. Dual database mode

In order to more conveniently and quickly complete the correctness verification, iot-benchmark also supports dual database mode.

1. For all the test scenarios mentioned above, unless otherwise specified, dual databases are supported. Please **start the test** in the `verification` project.
2. For the relevant test scenarios for correctness verification below, all must be run in dual database mode, and currently **only supports** iotdb-0.12, iotdb-0.13 and timescaledb.

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
DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
HOST=127.0.0.1
PORT=6667
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
GROUP_NUMBER=20
DEVICE_NUMBER=20
SENSOR_NUMBER=300
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=1
POINT_STEP=200
LOOP=1000
```

### 6.11.2. Start

Before starting the test, you need to start the IoTDB service on port 6667 of the machine, and start the TimescaleDB service on port 5432

Then you enter `iot-benchmark/verfication/target/iot-benchmark-verification` and run the following command to start Benchmark (currently only execute the following script in Unix/OS X system):

```sh
> ./benchmark.sh
```

### 6.11.3. Excute

After writing data is started, you can see the scrolling execution information, some of which are as follows:

```
...
2022-05-12 09:47:51,233 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-10 91.40% workload is done.
2022-05-12 09:47:51,233 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-13 90.90% workload is done.
2022-05-12 09:47:51,233 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-16 92.50% workload is done.
2022-05-12 09:47:51,233 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-2 91.90% workload is done.
...
```

When the test is over, the information written to the data set will be displayed at the end, as shown below:

```
2022-05-12 09:48:00,160 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished.
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=true
DBConfig=
  DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
  HOST=[127.0.0.1]
ANOTHER DBConfig=
  DB_SWITCH=TimescaleDB
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
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
CLIENT_NUMBER=20
LOOP=1000
BATCH_SIZE_PER_WRITE=1
START_TIME=2022-01-01T00:00:00+08:00
POINT_STEP=200
OP_MIN_INTERVAL=0
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
ENCODINGS=PLAIN/PLAIN/PLAIN/PLAIN/PLAIN/PLAIN
COMPRESSOR=SNAPPY
########### Other Param ###########
IS_DELETE_DATA=true
CREATE_SCHEMA=true
IS_COMPASSION=false
IS_POINT_COMPARISON=false
BENCHMARK_CLUSTER=false

---------------------------------------------------------------
main measurements:
Create schema cost 0.35 second
Test elapsed time (not include schema creation): 74.94 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)
INGESTION                40000                    12000000                 0                        0                        160136.98
PRECISE_POINT            0                        0                        0                        0                        0.00
TIME_RANGE               0                        0                        0                        0                        0.00
VALUE_RANGE              0                        0                        0                        0                        0.00
AGG_RANGE                0                        0                        0                        0                        0.00
AGG_VALUE                0                        0                        0                        0                        0.00
AGG_RANGE_VALUE          0                        0                        0                        0                        0.00
GROUP_BY                 0                        0                        0                        0                        0.00
LATEST_POINT             0                        0                        0                        0                        0.00
RANGE_QUERY_DESC         0                        0                        0                        0                        0.00
VALUE_RANGE_QUERY_DESC   0                        0                        0                        0                        0.00
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION                35.76       0.32        1.60        2.01        31.29       65.03       77.32       88.20       130.90      341.19      1457.70     72450.25
PRECISE_POINT            0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00
TIME_RANGE               0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00
VALUE_RANGE              0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00
AGG_RANGE                0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00
AGG_VALUE                0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00
AGG_RANGE_VALUE          0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00
GROUP_BY                 0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00
LATEST_POINT             0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00
RANGE_QUERY_DESC         0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00
VALUE_RANGE_QUERY_DESC   0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 6.12. Single-point query mode for correctness (dual database comparison)

In order to verify the correctness of the database data more efficiently, iot-benchmark provides to complete the correctness verification by comparing the data between the two databases.

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
DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
HOST=127.0.0.1
PORT=6667
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
GROUP_NUMBER=20
DEVICE_NUMBER=20
SENSOR_NUMBER=300
CLIENT_NUMBER=20
BATCH_SIZE_PER_WRITE=1
POINT_STEP=200
LOOP=1000
```

### 6.12.2. Start

Before starting the test, you need to start the IoTDB service on port 6667 of the machine, and start the TimescaleDB service on port 5432

Then you enter `iot-benchmark/verfication/target/iot-benchmark-verification` and run the following command to start Benchmark (currently only execute the following script in Unix/OS X system):

```sh
> ./benchmark.sh
```

### 6.12.3. Execute

After writing data is started, you can see the scrolling execution information, some of which are as follows:

```
...
2022-05-12 09:49:51,591 INFO  cn.edu.tsinghua.iot.benchmark.client.generate.GenerateDataDeviceClient:83 - All points of d_11 have been checked 
2022-05-12 09:49:51,596 INFO  cn.edu.tsinghua.iot.benchmark.client.generate.GenerateDataDeviceClient:83 - All points of d_7 have been checked 
2022-05-12 09:49:51,596 INFO  cn.edu.tsinghua.iot.benchmark.client.generate.GenerateDataDeviceClient:83 - All points of d_16 have been checked 
2022-05-12 09:49:51,596 INFO  cn.edu.tsinghua.iot.benchmark.client.generate.GenerateDataDeviceClient:83 - All points of d_12 have been checked 
...
```

When the test is over, the related information will be displayed at the end, as shown below:

```
2022-05-12 09:49:53,669 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=true
DBConfig=
  DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
  HOST=[127.0.0.1]
ANOTHER DBConfig=
  DB_SWITCH=TimescaleDB
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
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
CLIENT_NUMBER=20
LOOP=1000
BATCH_SIZE_PER_WRITE=1
START_TIME=2022-01-01T00:00:00+08:00
POINT_STEP=200
OP_MIN_INTERVAL=0
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
ENCODINGS=PLAIN/PLAIN/PLAIN/PLAIN/PLAIN/PLAIN
COMPRESSOR=SNAPPY
########### Other Param ###########
IS_DELETE_DATA=false
CREATE_SCHEMA=false
IS_COMPASSION=false
IS_POINT_COMPARISON=true
VERIFICATION_STEP_SIZE=10000
BENCHMARK_CLUSTER=false

---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 4.26 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)      
DEVICE_QUERY             40                       12000000                 0                        0                        2817131.26               
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
DEVICE_QUERY             918.19      701.50      709.92      786.73      857.27      1050.23     1093.01     1103.72     1163.73     1155.33     1154.39     1926.37     
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 6.13. Correctness function query mode (dual database comparison)

In order to verify the correctness of the database query more efficiently, iot-benchmark provides to complete the correctness verification by comparing the difference of the data query results between the two databases.

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
DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
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

Then you enter `iot-benchmark/verfication/target/iot-benchmark-verification` and run the following command to start Benchmark (currently only execute the following script in Unix/OS X system):

```sh
> ./benchmark.sh
```

### 6.13.3. Execute

After writing data is started, you can see the scrolling execution information, some of which are as follows:

```
...
2022-05-12 09:53:50,435 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-10 9.80% workload is done. 
2022-05-12 09:53:50,435 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-11 8.80% workload is done. 
2022-05-12 09:53:50,435 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-9 8.20% workload is done. 
2022-05-12 09:53:50,435 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-20 8.70% workload is done. 
...
```

When the test is over, the information written to the data set will be displayed at the end, as shown below:

```
2022-05-12 09:53:55,078 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=true
DBConfig=
  DB_SWITCH=IoTDB-013-SESSION_BY_TABLET
  HOST=[127.0.0.1]
ANOTHER DBConfig=
  DB_SWITCH=TimescaleDB
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
OPERATION_PROPORTION=0:1:1:1:1:1:1:1:1:1:1
CLIENT_NUMBER=20
LOOP=1000
BATCH_SIZE_PER_WRITE=1
START_TIME=2022-01-01T00:00:00+08:00
POINT_STEP=200
OP_MIN_INTERVAL=0
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
ENCODINGS=PLAIN/PLAIN/PLAIN/PLAIN/PLAIN/PLAIN
COMPRESSOR=SNAPPY
########### Query Param ###########
QUERY_DEVICE_NUM=1
QUERY_SENSOR_NUM=1
QUERY_INTERVAL=250000
STEP_SIZE=1
IS_RECENT_QUERY=false
########### Other Param ###########
IS_DELETE_DATA=false
CREATE_SCHEMA=false
IS_COMPASSION=true
IS_POINT_COMPARISON=false
BENCHMARK_CLUSTER=false

---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 5.66 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)      
INGESTION                0                        0                        0                        0                        0.00                     
PRECISE_POINT            4022                     2002                     0                        0                        353.77                   
TIME_RANGE               3966                     488814                   0                        0                        86377.75                 
VALUE_RANGE              3928                     500796                   0                        0                        88495.07                 
AGG_RANGE                4084                     3044                     0                        0                        537.90                   
AGG_VALUE                3824                     3824                     0                        0                        675.73                   
AGG_RANGE_VALUE          1977                     1003                     0                        0                        177.24                   
GROUP_BY                 4034                     31751                    0                        0                        5610.68                  
LATEST_POINT             4140                     4140                     0                        0                        731.57                   
RANGE_QUERY_DESC         4112                     519916                   0                        0                        91873.74                 
VALUE_RANGE_QUERY_DESC   3936                     511624                   0                        0                        90408.48                 
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION                0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT            1.17        0.13        0.31        0.40        0.65        1.24        2.32        3.69        7.74        18.33       93.15       341.72      
TIME_RANGE               1.51        0.14        0.30        0.42        0.75        1.62        3.25        4.76        11.06       28.13       95.53       417.16      
VALUE_RANGE              1.60        0.13        0.28        0.40        0.70        1.63        3.30        5.12        12.35       42.31       129.73      459.94      
AGG_RANGE                1.59        0.14        0.36        0.49        0.86        1.81        3.29        4.90        11.78       25.83       32.63       393.60      
AGG_VALUE                2.98        0.27        0.61        0.90        2.29        3.80        6.32        8.01        12.38       84.95       108.27      697.93      
AGG_RANGE_VALUE          2.20        0.26        0.46        0.70        1.49        2.73        4.59        6.62        12.75       27.20       32.76       300.54      
GROUP_BY                 1.67        0.15        0.42        0.55        0.93        1.90        3.50        5.19        10.90       18.57       98.43       426.65      
LATEST_POINT             1.11        0.10        0.27        0.34        0.54        1.06        2.22        3.35        10.11       18.99       20.13       295.61      
RANGE_QUERY_DESC         1.56        0.13        0.31        0.44        0.79        1.67        3.51        4.99        11.19       26.06       90.70       420.70      
VALUE_RANGE_QUERY_DESC   1.61        0.14        0.30        0.43        0.72        1.58        3.31        4.97        12.29       101.28      121.94      457.09      
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

1. Now verification **only support** IoTDB v0.12, IoTDB v0.13 and TimescaleDB
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
1. All the interfaces of iot-benchmark are in the core module.
2. The realization of all database tests of iot-benchmark are in each maven sub-project.
3. If you want to use an editor such as IDEA to run Benchmark:
    1. You can find TestEntrance in the test file directory under each maven subproject, and run the corresponding test.
    2. Taking IoTDB 0.13 as an example, you can run `iotdb-0.13/src/main/test/cn/edu/tsinghua/iotdb/benchmark/TestEntrance`

# 11. Related Article
Benchmark Time Series Database with iot-benchmark for IoT Scenarios

Arxiv: https://arxiv.org/abs/1901.08304

