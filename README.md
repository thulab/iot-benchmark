IoT Benchmark
---
![](https://img.shields.io/badge/platform-MacOS%20%7C%20Linux%20%7C%20Windows-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)

You can also read [中文版本](README-cn.md).

- [1. Overview](#1-overview)
- [2. Supported database types](#2-supported-database-types)
- [3. Quick Start](#3-quick-start)
  - [3.1. Prerequisites](#31-prerequisites)
  - [3.2. How to get it](#32-how-to-get-it)
  - [3.3. Quick Start](#33-quick-start)
- [4. Related articles](#4-related-articles)

# 1. Overview

![IoT Benchmark Architecture](https://github.com/apache/iotdb/assets/46039728/1fbf027d-0955-4de6-b727-57dbeaf2a5ab)

IoT-Benchmark is a benchmarking tool used to evaluate the performance of time series databases and real-time databases in the Industrial Internet of Things (IIoT) scenario. It has the following features:
1. **Cross-platform support**: Supports mainstream operating systems, and can start tests and obtain results through simple commands.
2. **Integrates multiple test functions in one, able to meet diverse test needs**:
   1. Generates periodic time series data according to the configuration and directly inserts and queries.
   2. Writes the generated data to the corresponding location on the disk.
   3. Loads data from the generated data set generated on the disk, writes and queries.
   4. Performs correctness verification tests on data and query results respectively.
3. **Supports multiple types of databases**: Supports mainstream time series databases and real-time database products on the market such as InfluxDB and IoTDB.
4. **Diversified test report generation**: Supports storing basic test information and results in various forms such as files and MySQL for further query or analysis.
5. **Test result visualization**: Integrates with Tableau to visualize test results.

# 2. Supported database types

Currently supports the following databases, versions and connection methods:

|       Database       |  Version   | Insert_Mode  |
| :------------------: | :--------: | :----------: |
|        IoTDB         |    v1.x    |     SDK      |
|       InfluxDB       | v1.x, v2.x |     SDK      |
|       QuestDB        |   v6.0.7   |     jdbc     |
| Microsoft SQL Server |  2016 SP2  |     jdbc     |
|   VictoriaMetrics    |  v1.64.0   | Http Request |
|        SQLite        |     --     |     jdbc     |
|       OpenTSDB       |   2.4.1    | Http Request |
|       KairosDB       |     --     | Http Request |
|     TimescaleDB      |     --     |     jdbc     |
|     TimescaleDB      |  Cluster   |     jdbc     |
|       TDengine       |  2.2.0.2   |     jdbc     |
|       TDengine       |   3.0.1    |     jdbc     |
|      PI Archive      |    2016    |     jdbc     |

# 3. Quick Start

## 3.1. Prerequisites

To use IoT Benchmark, you need:

1. Java 8
2. Maven: It is not recommended to use the mirror source. You can use the Alibaba Cloud mirror source in China.
3. The appropriate version of the database

Tips:

- The CSV recording mode can only be used in Linux systems to record relevant system information during the test.

- We recommend using MacOs or Linux systems. This article takes MacOS and Linux systems as examples. If you use Windows systems, please use the `benchmark.bat` script in the `conf` folder to start the benchmark.

## 3.2. How to get it

After ensuring that the above conditions are met, clone the source code from git:

```
git clone https://github.com/apache/iotdb.git
```

The default main branch is the master branch. If you want to use other branches, please enter the project root directory after cloning and use the following command to view all available branches:

```
git branch -a
```

After finding the branch name you want to work on, use the following command to switch to it:

```
git checkout [branch name]
```

Use the following command to complete the construction of iot-benchmark through Maven:

```
mvn clean package -Dmaven.test.skip=true
```
This command will compile the core module of iot-benchmark and all other related databases.hmark through Maven:

```
mvn clean package -Dmaven.test.skip=true
```
This command will compile the core module of iot-benchmark and all other related databases.

## 3.3. Quick Start

Tip: All test results in this document are from the following environment:

```
CPU: I7-11700
Memory: 32G DDR4
System disk: 512G SSD (INTEL SSDPEKNU512GZ)
Data disk: 2T HDD (WDC WD40EZAZ-00SF3B0)
```

After the compilation is completed, taking IoTDB v1.0 as an example, **you need to first start the corresponding version of IoTDB service on port 6667 of the local machine**. (If you still have questions about using IoTDB, please refer to the instructions in [IoTDB_README.md](https://github.com/apache/iotdb/blob/master/README_ZH.md)). After successfully starting the IoTDB service, you can go to the `iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0/iot-benchmark-iotdb-1.0` folder and use `./benchmark.sh` to start the test of IoTDB v1.0. We recommend using the matching version for testing to achieve the best results.

[Test configuration file](https://github.com/supersshhhh/iot-benchmark/blob/patch-1/Testconfigurations.md)

```

cd iotdb-1.0/target/iot-benchmark-iotdb-1.0/iot-benchmark-iotdb-1.0
./benchmark.sh
```

This will perform a write test using the default configuration. After the test starts, if the execution is correct, you will see the scrolling test execution information, some of which are as follows:

```
...
2022-05-08 14:26:36,478 INFO cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-4 17.10% workload is done.
2022-05-08 14:26:41,479 INFO cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-13 56.59% workload is done.
2022-05-08 14:26:41,479 INFO cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-2 18.01% workload is done.
2022-05-08 14:26:41,480 INFO cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-14 54.01% workload is done.
...
```

When the test is finished, the statistics of this test will be displayed as follows:

```
...
2022-05-08 14:40:54,243 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=false
DBConfig=
  DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

The output contains the overall information of the test, including:
+ Main configuration information
+ Total test time Test elapsed time
+ Total metadata creation time
+ Result matrix
  + okOperation: The number of Request/SQL for different operations that were successfully executed
  + okPoint: The number of data points that were successfully inserted or the number of data points that successfully returned query results
  + failOperation: The number of Request/SQL for different operations that failed to execute
  + failPoint: The number of data points that failed to insert (this value is always 0 for queries)
  + throughput: equal to ```okPoint / Test elapsed time```
  + <a href = "https://y8dp9fjm8f.feishu.cn/file/boxcndtRvCh3qRNScNm8J5XERWf">Detailed parameter description</a>
+ Millisecond latency statistics for different operations
  + Among them, ```SLOWEST_THREAD``` is the maximum cumulative operation time length in the client thread

All the above information will be recorded in the ```logs``` folder of the running device.

The configuration files are stored in `iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0/iot-benchmark-iotdb-1.0conf`. Of course, you can also find the configuration files of other supported databases in similar paths. Edit the file to define the test type and related configuration. **Please note that before each test, you must change the DB_SWITCH parameter in the configuration file to match the database to be tested. The corresponding relationship and possible values ​​are as follows:**

|       Database       | Version  | Corresponding sub-project |                                                  DB_SWITCH                                                   |
| :------------------: | :------: | :-----------------------: | :----------------------------------------------------------------------------------------------------------: |
|  IoTDB(1.0/1.1/1.3)  |   1.x    |         iotdb-1.x         | IoTDB-1x0-JDBC<br>IoTDB-1x0-SESSION_BY_TABLET<br>IoTDB-1x0-SESSION_BY_RECORD<br>IoTDB-1x0-SESSION_BY_RECORDS |
|       InfluxDB       |   1.x    |         influxdb          |                                                   InfluxDB                                                   |
|       InfluxDB       |   2.0    |       influxdb-2.0        |                                                 InfluxDB-2.0                                                 |
|       QuestDB        |  6.0.7   |          questdb          |                                                   QuestDB                                                    |
| Microsoft SQL Server | 2016 SP2 |        mssqlserver        |                                                 MSSQLSERVER                                                  |
|   VictoriaMetrics    |  1.64.0  |      victoriametrics      |                                               VictoriaMetrics                                                |
|     TimescaleDB      |    --    |        timescaledb        |                                                 TimescaleDB                                                  |
|     TimescaleDB      | Cluster  |    timescaledb-cluster    |                                             TimescaleDB-Cluster                                              |
|        SQLite        |    --    |          sqlite           |                                                    SQLite                                                    |
|       OpenTSDB       |    --    |         opentsdb          |                                                   OpenTSDB                                                   |
|       KairosDB       |    --    |         kairosdb          |                                                   KairosDB                                                   |
|       TDengine       | 2.2.0.2  |         TDengine          |                                                   TDengine                                                   |
|       TDengine       |  3.0.1   |         TDengine          |                                                  TDengine-3                                                  |
|      PI Archive      |   2016   |         PIArchive         |                                                  PIArchive                                                   |

* For detailed instructions on using different databases, see [Tested Database Example Instructions](./docs/DifferentTestDatabase.md)

At the same time, you can also change the BENCHMARK_WORK_MODE parameter to adjust the running mode of iot-benchmark. Currently, the following are supported:

|       Mode name        |  BENCHMARK_WORK_MODE  | Mode content                                                                                                                     |
| :--------------------: | :-------------------: | :------------------------------------------------------------------------------------------------------------------------------- |
| Conventional test mode |  testWithDefaultPath  | Supports mixed loads of multiple read and write operations                                                                       |
|   Generate data mode   |   generateDataMode    | Benchmark generates data sets to the FILE_PATH path                                                                              |
| Correctness write mode | verificationWriteMode | Loads data sets from the FILE_PATH path for writing. Currently, IoTDB v1.0 and later versions are supported                      |
| Correctness query mode | verificationQueryMode | Loads data sets from the FILE_PATH path for comparison with the database. Currently, IoTDB v1.0 and later versions are supported |

* For more mode details, refer to [Overview of different test modes](./docs/DifferentTestMode.md), [Sample configuration of different test modes](./docs//DifferentTestModeConfig.md)
* For other variable parameter configurations and annotations, please refer to [config.properties](configuration/conf/config.properties), which will not be expanded here.

# 4. Related articles
Benchmark Time Series Database with iot-benchmark for IoT Scenarios

Arxiv: https://arxiv.org/abs/1901.08304
