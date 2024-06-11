# 1. iot-benchmark

![](https://img.shields.io/badge/platform-MacOS%20%7C%20Linux%20%7C%20Windows-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)

You can also read [中文版本](README-cn.md).

# 2. Table of Contents

- [1. iot-benchmark](#1-iot-benchmark)
- [2. Table of Contents](#2-Table-of-Contents)
- [3. Overview](#3-Overview)
- [4. Main Features](#4-Main-Features)
- [5. Installation](#5-Installation)
  - [5.1. Prerequisites of environment](#51-Prerequisites-of-environment)
  - [5.2. Process of installation](#52-Process-of-installation)
- [6. Use of iot-benchmark](#6-Use-of-iot-benchmark)
  - [6.1 Quick Start](#61-Quick-Start)
  - [6.2 Different operation modes of iot-benchmark](#62-Different-operation-modes-of-iot-benchmark)
    - [6.2.1 General test mode write (single database)](#621-General-test-mode-write-single-database)
  - [6.2.2 Query in regular test mode (single database, no system records)](#622-Query-in-regular-test-mode-single-database-no-system-records)
  - [6.2.3 Read-write mixed mode in regular test mode (single database)](#623-Read-write-mixed-mode-in-regular-test-mode-single-database)
  - [6.2.4 Read-write mixed mode in regular test mode (single database, query the most recently written data)](#624-Read-write-mixed-mode-in-regular-test-mode-single-database-query-the-most-recently-written-data)
  - [6.2.5 Use system records in regular test mode (single database)](#625-Use-system-records-in-regular-test-mode-single-database)
  - [6.2.6 Test process persistence in regular test mode (single database)](#626-Test-process-persistence-in-regular-test-mode-single-database)
  - [6.2.7 Generate data mode](#627-Generate-data-mode)
  - [6.2.8 Correctness write mode (single database, external data set)](#628-Correctness-write-mode-single-database-external-data-set)
  - [6.2.9 Correctness single-point query mode (single database, external data set)](#629-Correctness-single-point-query-mode-single-database-external-data-set)
  - [6.2.10 Dual database mode](#6210-Dual-database-mode)
  - [6.2.11 Regular test mode writing (dual database)](#6211-Regular-test-mode-writing-dual-database)
  - [6.2.12 Correctness single point query mode (dual database comparison)](#6212-Correctness-single-point-query-mode-dual-database-comparison)
  - [6.2.13 Correctness function query mode (double database comparison)](#6213-Correctness-function-query-mode-double-database-comparison)
- [7. Testing other databases with IoTDB Benchmark (partial)](#7-Testing-other-databases-with-IoTDB-Benchmark-partial)
  - [7.1. Testing InfluxDB v1.x](#71-Testing-InfluxDB-v1x)
  - [7.2. Testing InfluxDB v2.0](#72-Testing-InfluxDB-v20)
  - [7.3. Testing Microsoft SQL Server](#73-Testing-Microsoft-SQL-Server)
  - [7.4. Testing QuestDB](#74-Testing-QuestDB)
  - [7.5. Testing SQLite](#75-Testing-SQLite)
  - [7.6. Testing Victoriametrics](#76-Testing-Victoriametrics)
  - [7.7. Testing TimeScaleDB](#77-Testing-TimeScaleDB)
  - [7.8. Test PI Archive](#78-Test-PI-Archive)
  - [7.9. Test TDengine](#79-Test-TDengine)
- [8. Further explanation of correctness verification](#8-Further-explanation-of-correctness-verification)
- [9. Automation Script](#9-Automation-Script)
  - [9.1. One-click Startup Script](#91-One-click-Startup-Script)
  - [9.2. Automatically execute multiple tests](#92-Automatically-execute-multiple-tests)
    - [9.2.1. Configure routine](#921-Configure-routine)
    - [9.2.2. Start the test](#922-Start-the-test)
- [10. Related articles](#10-Related-articles)


# 3. Overview

IoT-Benchmark is a benchmarking tool used to evaluate the performance of time series databases and real-time databases in Industrial Internet of Things (IIoT) scenarios.

Currently supports the following databases, versions, and connection methods:

|       Database       | Version  |               Insert_Mode               |
| :------------------: | :------: | :-------------------------------------: |
|        IoTDB         |   v1.x   |                  jdbc                   |
|       InfluxDB       |   v1.x   |                   SDK                   |
|       InfluxDB       |   v2.0   |                   SDK                   |
|       QuestDB        |  v6.0.7  |                  jdbc                   |
| Microsoft SQL Server | 2016 SP2 |                  jdbc                   |
|   VictoriaMetrics    | v1.64.0  |              Http Request               |
|        SQLite        |    --    |                  jdbc                   |
|       OpenTSDB       |  2.4.1   |              Http Request               |
|       KairosDB       |    --    |              Http Request               |
|     TimescaleDB      |    --    |                  jdbc                   |
|     TimescaleDB      | Cluster  |                  jdbc                   |
|       TDengine       | 2.2.0.2  |                  jdbc                   |
|       TDengine       |  3.0.1   |                  jdbc                   |
|      PI Archive      |   2016   |                  jdbc                   |

# 4. Main Features

The features of iot-benchmark are as follows:

1. Easy to use: iot-benchmark is a tool that combines multiple testing functions, and users do not need to switch between different tools.

2. Multiple data insertion and testing modes:

1. Generate periodic time series data according to the configuration and insert and query directly.

2. Write the generated data to the corresponding location on the disk.

3. Load data from the generated data set generated on the disk, write and query.

4. Perform correctness verification tests on data and query results respectively.

3. Test reports and results: Support storage of test information and results for further query or analysis.

4. Visualize test results: Integrate with Tableau to visualize test results.

# 5. Installation

## 5.1. Prerequisites of environment

1. Java 8
2. Maven: It is not recommended to use the mirror source. Alibaba Cloud mirror source can be used in China.
3. Appropriate version of database
1. Apache IoTDB >= v1.0 ([How to obtain](https://github.com/apache/iotdb))
2. Other corresponding versions of database

Tips: 
--CSV recording mode can only be used in Linux system to record relevant system information during the test.
--We recommend using MacOs or Linux system. This article takes MacOS and Linux system as examples. If you use Windows system, please use the `benchmark.bat` script in the `conf` folder to start the benchmark.

## 5.2. Process of installation

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
This command will compile the core module of iot-benchmark and all other related databases.


# 6. Use of iot-benchmark

## 6.1 Quick Start

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

| Database | Version | Corresponding sub-project | DB_SWITCH |
| :------------------: | :------: | :-----------------: | :------------------------------------------------------------------------------------------: |
| IoTDB(1.0/1.1/1.3) | 1.x | iotdb-1.x | IoTDB-1x0-JDBC<br>IoTDB-1x0-SESSION_BY_TABLET<br>IoTDB-1x0-SESSION_BY_RECORD<br>IoTDB-1x0-SESSION_BY_RECORDS |
| InfluxDB | 1.x | influxdb | InfluxDB |
| InfluxDB | 2.0 | influxdb-2.0 | InfluxDB-2.0 |
| QuestDB | 6.0.7 | questdb | QuestDB |
| Microsoft SQL Server | 2016 SP2 | mssqlserver | MSSQLSERVER |
| VictoriaMetrics | 1.64.0 | victoriametrics | VictoriaMetrics |
| TimescaleDB | -- | timescaledb | TimescaleDB |
| TimescaleDB | Cluster | timescaledb-cluster | TimescaleDB-Cluster |
| SQLite | -- | sqlite | SQLite |
| OpenTSDB | -- | opentsdb | OpenTSDB |
| KairosDB | -- | kairosdb | KairosDB |
| TDengine | 2.2.0.2 | TDengine | TDengine |
| TDengine | 3.0.1 | TDengine | TDengine-3 |
| PI Archive | 2016 | PIArchive | PIArchive |

At the same time, you can also change the BENCHMARK_WORK_MODE parameter to adjust the running mode of iot-benchmark. Currently, the following are supported:

| Mode name | BENCHMARK_WORK_MODE | Mode content |
| :------------: | :-------------------: | :------------------------------------------------------------------------------- |
| Conventional test mode | testWithDefaultPath | Supports mixed loads of multiple read and write operations |
| Generate data mode | generateDataMode | Benchmark generates data sets to the FILE_PATH path |
| Correctness write mode | verificationWriteMode | Loads data sets from the FILE_PATH path for writing. Currently, IoTDB v1.0 and later versions are supported |
| Correctness query mode | verificationQueryMode | Loads data sets from the FILE_PATH path for comparison with the database. Currently, IoTDB v1.0 and later versions are supported |

For other variable parameter configurations and annotations, please see [config.properties](configuration/conf/config.properties), which will not be expanded here.

## 6.2 Different operation modes of iot-benchmark

Again, before you start any new test case, you need to confirm whether the configuration information in the configuration file ```config.properties``` meets your expectations.

### 6.2.1 General test mode write (single database)

Assume that the workload parameters are:

```
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
| Measurement/StorageGroup | tag/device | field/sensor | concurrency | batch size | point interval(ms) | loop |
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
| 10 | 50 | 500 | 20 | 100 | 200 | 10000 |
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
```

Note: The total number of time series in this configuration is: ```device * sensor = 25,000```, and the number of points in each time series is ```batch size * loop = 20,000```,
The total number of data points is ```deivce * sensor * batch size * loop = 500,000,000```. The space occupied by each data point can be estimated as 16 bytes, so the total size of the raw data is 8G.

Then, the ```config.properties``` file needs to be modified as shown below. Please remove the ```#``` before the corresponding configuration item after completing the modification to ensure that the changes take effect:

```properties
HOST=127.0.0.1
PORT=6667
DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

If you want to write data in a disorderly manner, you need to modify the following properties of the `config.properties` file:

```
# Whether to write in disorder
IS_OUT_OF_ORDER=true
# There are currently two types of disorderly writing modes
# POISSON The disorderly mode according to Poisson distribution
# BATCH The batch insertion disorderly mode
OUT_OF_ORDER_MODE=BATCH
# The proportion of data written in disorder
OUT_OF_ORDER_RATIO=0.5
# Is it equal time stamp
IS_REGULAR_FREQUENCY=true
# The expectation and variance of Poisson distribution
LAMBDA=2200.0
# The maximum value of random numbers in the Poisson distribution model
MAX_K=170000
```

Some basic parameter modification operations have been introduced here, and the relevant settings will be omitted in the following part. If necessary, please enter the following page to view.
[TestConfigurations](https://github.com/supersshhhh/iot-benchmark/blob/patch-1/Testconfigurations.md)

## 6.2.2 Query in regular test mode (single database, no system records)

In addition to writing data, regular test mode can also be used to query data.

## 6.2.3 Read-write mixed mode in regular test mode (single database)

Regular test mode can support users to perform mixed read-write tests. It should be noted that the timestamps of mixed read-write in this scenario all start from the **write start time**.

## 6.2.4 Read-write mixed mode in regular test mode (single database, query the most recently written data)

Regular test mode can support users to perform mixed read-write tests (query the most recently written data). It should be noted that the query time range in this scenario is the data adjacent to the left side of the current maximum write timestamp.

## 6.2.5 Use system records in regular test mode (single database)

IoTDB Benchmark supports you to use the database to store system data during the test. Currently, it supports the use of CSV records.

## 6.2.6 Test process persistence in regular test mode (single database)

For subsequent analysis, iot-benchmark can store test information in the database (if you don't want to store test data, set ```TEST_DATA_PERSISTENCE=None```)

## 6.2.7 Generate data mode

In order to generate reusable data sets, iot-benchmark provides a mode for generating data sets, generating data sets to FILE_PATH for subsequent use in correctness write mode and correctness query mode.

## 6.2.8 Correctness write mode (single database, external data set)

In order to verify the correctness of data set writing, you can use this mode to write the data set generated in the generate data mode. Currently, this mode only supports IoTDB v1.0 and later versions and InfluxDB v1.x

## 6.2.9 Correctness single-point query mode (single database, external data set)

Before running this mode, you need to use the correctness write mode to write data to the database. To verify the correctness of the dataset written, you can use this mode to query the dataset written to the database. Currently, this mode only supports IoTDB v1.0 and InfluxDB v1.x.

## 6.2.10 Dual database mode

In order to complete the correctness verification more conveniently and quickly, iot-benchmark also supports dual database mode.

1. For all the test scenarios mentioned above, unless otherwise specified, dual databases are supported. Please **start the test** in the `verification` project.

2. For the relevant test scenarios for correctness verification below, they must be run in dual database mode, and currently only IoTDB v1.0 and later versions and timescaledb are supported.

## 6.2.11 Regular test mode writing (dual database)

In order to perform the correctness verification below, you first need to write the data to two databases.

## 6.2.12 Correctness single point query mode (dual database comparison)

In order to verify the correctness of database data more efficiently, iot-benchmark provides correctness verification by comparing the data between two databases. Note that before performing this test, please use the regular test mode writing (dual database) mentioned above to complete the database writing. Currently, it is recommended to use the JDBC method.

## 6.2.13 Correctness function query mode (double database comparison)

In order to more efficiently verify the correctness of database queries, iot-benchmark provides correctness verification by comparing the differences in data query results between two databases.

Note:

1. Before performing this test, please use the general test mode of writing (double database) mentioned above to complete database writing.
2. The value of LOOP cannot be too large, satisfying: LOOP(query) * QUERY_INTERVAL(query) * DEVICE_NUMBER(write) <= LOOP(write) * POINT_STEP(write)

# 7. Testing other databases with IoTDB Benchmark (partial)

## 7.1. Testing InfluxDB v1.x
[Quick Guide](influxdb/README.md)

## 7.2. Testing InfluxDB v2.0
[Quick Guide](influxdb-2.0/README.md)

## 7.3. Testing Microsoft SQL Server
[Quick Guide](mssqlserver/README.md)

## 7.4. Testing QuestDB
[Quick Guide](questdb/README.md)

## 7.5. Testing SQLite
[Quick Guide](sqlite/README.md)

## 7.6. Testing Victoriametrics
[Quick Guide](victoriametrics/README.md)

## 7.7. Testing TimeScaleDB
[Quick Guide](timescaledb/README.md)

## 7.8. Test PI Archive

[Quick Guide](./pi/README.md)

## 7.9. Test TDengine
[Quick Guide](./tdengine/README.md)

# 8. Further explanation of correctness verification
1. Currently, the correctness verification part only supports IoTDB v1.0 and later versions and TimeScaleDB
2. [Quick Guide](verification/README.md)

# 9. Automation Script

## 9.1. One-click Startup Script
You can start IoTDB, monitoring IoTDB Benchmark and testing IoTDB Benchmark in one click through the `cli-benchmark.sh` script, but please note that the script will clean up **all data** in IoTDB when it starts, so please use it with caution.

First, you need to modify the `IOTDB_HOME` parameter in `cli-benchmark.sh` to the folder where your local IoTDB is located.

Then you can start the test using the script

```sh
> ./cli-benchmark.sh
```

After the test is completed, you can view the test-related logs in the `logs` folder and the monitoring-related logs in the `server-logs` folder.

## 9.2. Automatically execute multiple tests

Usually, a single test is meaningless unless compared with other test results. Therefore, we provide an interface to execute multiple tests with a single start.

### 9.2.1. Configure routine

Each line of this file should be a parameter that will change during each test (otherwise it becomes a duplicate test). For example, the "routine" file is:

```
LOOP=10 DEVICE_NUMBER=100 TEST
LOOP=20 DEVICE_NUMBER=50 TEST
LOOP=50 DEVICE_NUMBER=20 TEST
```

Then the test process with 3 LOOP parameters of 10, 20, and 50 is executed in sequence.

> Note:
> You can change multiple parameters in each test using the format of "LOOP=20 DEVICE_NUMBER=10 TEST", and unnecessary space is not allowed. The keyword "TEST" means a new test starts. If you change different parameters, the changed parameters will be retained in the next test.

### 9.2.2. Start the test

After configuring the file routine, you can start the multi-test task by starting the script:

```sh
> ./rep-benchmark.sh
```

Then the test information will be displayed in the terminal.

> Note:
> If you close the terminal or lose the connection with the client machine, the test process will terminate. If the output is piped to a terminal, it is the same as in any other case.


Using this interface usually takes a long time, and you may want to execute the test process as a daemon. To do this, you can start the test task as a daemon through the startup script:

```sh
> ./rep-benchmark.sh > /dev/null 2>&1 &
```

In this case, if you want to know what happened, you can view the log information through the following command:

```sh
> cd ./logs
> tail -f log_info.log
```

# 10. Related articles
Benchmark Time Series Database with iot-benchmark for IoT Scenarios

Arxiv: https://arxiv.org/abs/1901.08304
