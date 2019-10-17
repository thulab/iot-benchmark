# IoTDB-Benchmark
![](https://img.shields.io/badge/platform-MacOS%20%7C%20Linux-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)

# Table of Contents

<!-- MarkdownTOC autolink="true" -->

- [Overview](#overview)
- [Main Features](#main-features)
- [Prerequisites](#prerequisites)
- [Build](#build)
- [Quick Start](#quick-start)
	- [Data Ingestion Test](#data-ingestion-test)
		- [Configure](#configure)
		- [Start \(Without Server System Information Recording\)](#start-without-server-system-information-recording)
		- [Execute](#execute)
- [Other Cases](#other-cases)
	- [Query Test](#query-test)
		- [Configure](#configure-1)
		- [Start \(Without Server System Information Recording\)](#start-without-server-system-information-recording-1)
		- [Execute](#execute-1)
	- [Test IoTDB With Server System Information Recording](#test-iotdb-with-server-system-information-recording)
		- [Configure](#configure-2)
		- [Start \(With Server System Information Recording\)](#start-with-server-system-information-recording)
	- [Test InfluxDB](#test-influxdb)
		- [Configure](#configure-3)
		- [Start](#start)
	- [Perform Multiple Tests Automatically](#perform-multiple-tests-automatically)
		- [Configure](#configure-4)
		- [Start](#start-1)
	- [MySQL Integration](#mysql-integration)
- [Related Article](#related-article)

<!-- /MarkdownTOC -->

# Overview

IoTDB-benchmark is a tool for benchmarking IoTDB against other databases and time series solutions.

Databases currently supported:

+ IoTDB
+ InfluxDB
+ KairosDB
+ TimescaleDB

# Main Features

IoTDB-benchmark's features are as following:

1. Easy to use. IoTDB-benchmark is a tool combined multiple testing functions so users do not need to switch different tools. 
2. Different writing test model: Load existing data from file; Generate real-time data continuously.
3. Supporting storing testing information and results for further query or analysis.
4. Integration with Jenkins to report test result automatically.

# Prerequisites

To use IoTDB-benchmark, you need to have:

1. Java 8
2. Maven (If you want to compile and install IoTDB from source code)
3. Apache IoTDB >= v0.8.0 ([https://github.com/apache/incubator-iotdb](https://github.com/apache/incubator-iotdb))
4. InfluxDB >= 1.3.7
5. other database system under test
6. sysstat (If you want to record system information of DB-server during test)

# Build

You can build IoTDB-benchmark using Maven:

```
mvn clean package -Dmaven.test.skip=true
```

> This step is not necessary since the ```benchmark.sh``` script will build the project every time. You can comment the corresponding command to save time.

# Quick Start

This short guide will walk you through the basic process of using IoTDB-benchmark.

## Data Ingestion Test

### Configure

Before starting any new test case, you need to config the configuration files ```config.properties``` first which is in ```iotdb-benchmark/conf```. For your convenience, we have already set the default config for the following demonstration.

Suppose you are going to test data ingestion performance of IoTDB. You have installed IoTDB dependencies and launched a IoTDB server with default settings. The IP of the server is 127.0.0.1. Suppose the workload parameters are:

```
+--------------------------+------------+--------------+-------------+------------+-------------------+------+
| Measurement/StorageGroup | tag/device | field/sensor | concurrency | batch size | point interval(s) | loop |
+--------------------------+------------+--------------+-------------+------------+-------------------+------+
|            20            |    20      |      300     |     20      |      1     |         5         | 1000 |
+--------------------------+------------+--------------+-------------+------------+-------------------+------+
```

edit the corresponding parameters in the ```conf/config.properties``` file as following:

```
HOST=127.0.0.1
PORT=6667
DB_SWITCH=IoTDB
BENCHMARK_WORK_MODE=testWithDefaultPath
OPERATION_PROPORTION=2:0:0:0:0:0:0:0:0
GROUP_NUMBER=20
DEVICE_NUMBER=20
SENSOR_NUMBER=300
CLIENT_NUMBER=20
BATCH_SIZE=1
POINT_STEP=5000
LOOP=1000
```

> NOTE:
Other irrelevant parameters are omitted. You can just set as default. We will cover them later in other cases.

### Start (Without Server System Information Recording)

Running the startup script, currently we only support Unix/OS X system: 

```
> ./benchmark.sh
```

### Execute 

Now after launching the test, you will see testing information rolling like following: 

```
···
2019-10-15 19:36:36,405 INFO  cn.edu.tsinghua.iotdb.benchmark.client.BaseClient:48 - pool-1-thread-2 89.00% syntheticWorkload is done. 
2019-10-15 19:36:36,405 INFO  cn.edu.tsinghua.iotdb.benchmark.client.BaseClient:48 - pool-1-thread-7 86.00% syntheticWorkload is done. 
···
```

When test is done, the last output of the test information will be like following: 

```
2019-10-15 21:18:03,148 INFO  cn.edu.tsinghua.iotdb.benchmark.App:230 - All clients finished. 
----------------------Main Configurations----------------------
DB_SWITCH: IoTDB
OPERATION_PROPORTION: 2:0:0:0:0:0:0:0:0
IS_CLIENT_BIND: true
CLIENT_NUMBER: 20
GROUP_NUMBER: 20
DEVICE_NUMBER: 20
SENSOR_NUMBER: 300
BATCH_SIZE: 10
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OVERFLOW: false
OVERFLOW_MODE: 0
OVERFLOW_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 36.10 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           20000               60000000            0                   0                   1662182.15          
PRECISE_POINT       0                   0                   0                   0                   0.00                
TIME_RANGE          0                   0                   0                   0                   0.00                
VALUE_RANGE         0                   0                   0                   0                   0.00                
AGG_RANGE           0                   0                   0                   0                   0.00                
AGG_VALUE           0                   0                   0                   0                   0.00                
AGG_RANGE_VALUE     0                   0                   0                   0                   0.00                
GROUP_BY            0                   0                   0                   0                   0.00                
LATEST_POINT        0                   0                   0                   0                   0.00                
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MID_AVG     MIN         P10         P25         MEDIAN      P75         P90         P95         P99         MAX         SLOWEST_THREAD     
INGESTION           33.45       14.61       5.24        8.58        9.05        10.07       15.14       98.55       157.05      265.06      5805.33     35222.02    
PRECISE_POINT       0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
TIME_RANGE          0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE         0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_RANGE           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_VALUE           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_RANGE_VALUE     0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
GROUP_BY            0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
LATEST_POINT        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
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
+ The latency statistics of different operations in millisecond 
+ ```SLOWEST_THREAD``` is the max accumulative operation time-cost among the client threads

All these information will be logged in ```iotdb-benchmark/logs``` directory on client server.

Till now, we have already complete the writing test case without server information recording. For more advanced usage of IoTDB-benchmark, please follow the ```Other Case``` instruction.

# Other Cases

## Query Test

### Configure

Edit the corresponding parameters in the ```conf/config.properties``` file as following:

```
### Main Data Ingestion and Query Shared Parameters
HOST=127.0.0.1
PORT=6667
DB_SWITCH=IoTDB
BENCHMARK_WORK_MODE=testWithDefaultPath
OPERATION_PROPORTION=0:1:2:1:1:1:1:1:1
GROUP_NUMBER=20
DEVICE_NUMBER=20
SENSOR_NUMBER=300
CLIENT_NUMBER=20
BATCH_SIZE=1
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
TIME_UNIT=20000
```

> NOTE:
Usually the query test is performed after the data ingestion test. Of course you can add ingestion operation at the same time by setting ```OPERATION_PROPORTION=INGEST:1:2:1:1:1:1:1:1``` as long as ```INGEST``` is not zero, since the parameter ```OPERATION_PROPORTION``` is to control the proportion of different operations including ingestion and query operations.


### Start (Without Server System Information Recording)

Running the startup script: 

```
> ./benchmark.sh
```

### Execute 

Now after launching the test, you will see testing information rolling like following: 

```
···
2019-10-15 20:02:16,141 INFO  cn.edu.tsinghua.iotdb.benchmark.client.BaseClient:48 - pool-1-thread-19 97.40% syntheticWorkload is done. 
2019-10-15 20:02:16,141 INFO  cn.edu.tsinghua.iotdb.benchmark.client.BaseClient:48 - pool-1-thread-2 97.10% syntheticWorkload is done. 
···
```

When test is done, the last testing information will be like the following: 

```
2019-10-15 21:22:04,751 INFO  cn.edu.tsinghua.iotdb.benchmark.App:230 - All clients finished. 
----------------------Main Configurations----------------------
DB_SWITCH: IoTDB
OPERATION_PROPORTION: 0:1:2:1:1:1:1:1:1
IS_CLIENT_BIND: true
CLIENT_NUMBER: 20
GROUP_NUMBER: 20
DEVICE_NUMBER: 20
SENSOR_NUMBER: 300
BATCH_SIZE: 10
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OVERFLOW: false
OVERFLOW_MODE: 0
OVERFLOW_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 112.87 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           0                   0                   0                   0                   0.00                
PRECISE_POINT       2237                2237                0                   0                   19.82               
TIME_RANGE          4452                227052              0                   0                   2011.63             
VALUE_RANGE         2138                109037              0                   0                   966.04              
AGG_RANGE           2184                2184                0                   0                   19.35               
AGG_VALUE           2173                2173                0                   0                   19.25               
AGG_RANGE_VALUE     2327                2327                0                   0                   20.62               
GROUP_BY            2283                29679               0                   0                   262.95              
LATEST_POINT        2206                2206                0                   0                   19.54               
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MID_AVG     MIN         P10         P25         MEDIAN      P75         P90         P95         P99         MAX         SLOWEST_THREAD     
INGESTION           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT       67.98       67.36       6.07        37.51       49.49       56.64       96.85       100.43      112.30      149.98      197.27      8466.59     
TIME_RANGE          69.00       68.81       7.10        37.64       49.71       58.47       97.36       100.84      110.94      150.15      216.05      16514.83    
VALUE_RANGE         102.39      100.44      21.54       51.13       84.52       99.55       126.55      149.79      159.25      200.23      292.96      13785.02    
AGG_RANGE           69.54       68.90       7.21        38.28       49.69       59.69       96.83       100.82      112.80      152.29      215.73      8658.98     
AGG_VALUE           105.70      103.11      17.79       52.16       85.99       99.95       137.50      150.50      185.90      235.07      296.95      14625.04    
AGG_RANGE_VALUE     104.19      102.30      15.60       52.21       86.05       99.82       134.94      150.11      162.78      203.62      292.38      13979.48    
GROUP_BY            67.85       67.60       7.19        34.90       49.43       57.08       96.59       100.73      109.12      150.79      229.81      8979.77     
LATEST_POINT        88.29       85.56       14.57       49.20       55.31       94.88       100.59      142.03      149.59      196.11      286.50      11234.28    
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## Test IoTDB With Server System Information Recording

### Configure

If you are using IoTDB-benchmark for the first time, you neet to config testing related environment in startup scripts 'cli-benchmark'.
Suppose your IoTDB server IP is 192.168.130.9 and your test client server which installed IoTDB-benchmark has authorized ssh access to the IoTDB server.
Current version of information recording is dependent on iostat. Please make sure iostat is installed in IoTDB server.

Configure 'config.properties'
Suppose you are using the same parameters as in Quick Start case. The new parameters you should add are INTERVAL and DB_DATA_PATH like:

```
INTERVAL=0
DB_DATA_PATH=/home/liurui
```

INTERVAL=0 means the server information recording with the minimal interval 2 seconds. If you set INTERVAL=n then the interval will be n+2 seconds since the recording process require least 2 seconds. You may want to set the INTERVAL longer when conducting long testing.
DB_DATA_PATH is the directory where to touch the file 'log_stop_flag' to stop the recording process. Therefore it has to be accessible by IoTDB-benchmark. It is also the data directory of IoTDB.

Configure 'cli-benchmark.sh'

```
IOTDB_HOME=/home/liurui/github/iotdb/iotdb/bin
REMOTE_BENCHMARK_HOME=/home/liurui/github/iotdb-benchmark
HOST_NAME=liurui
```

+ IOTDB_HOME: The bin directory where you installed IoTDB on DB-server.
+ REMOTE_BENCHMARK_HOME: The directory where you installed IoTDB-benchmark on DB-server.
+ HOST_NAME: The host name of DB-server.

### Start (With Server System Information Recording)

After configuring the first-time test environment, you can just launch the test by startup script:

```
> ./cli-benchmark.sh
```

The system information will be logged in 'iotdb-benchmark/logs' on DB-server.

## Test InfluxDB 

If you followed the cases above, this will be very easy.

### Configure

Configure 'config.properties'
Suppose you are using the same parameters as in Quick Start case. The only parameter you should change is DB_SWITCH and there are two new parameters:

```
DB_SWITCH=InfluxDB
DB_URL=http://127.0.0.1:8086
DB_NAME=test
```

> NOTE:
The benchmark can automatically initial database(if ```IS_DELETE_DATA=true```), then there is no need for executing SQL like ```drop database test```.

### Start 

After configuring the first-time test environment, you can just launch the test by startup script:

```
> ./benchmark.sh
```

Then one test process is on going.

## Perform Multiple Tests Automatically

Usually a single test is meaningless unless it is compared with other test results. Therefore we provide a interface to execute multiple tests by one launch.

### Configure

Configure 'routine'
Each line of this file should be the parameters each test process will change(otherwise it becomes replication test). For example, the 'routine' file is:

```
LOOP=10 DEVICE_NUMBER=100 TEST
LOOP=20 DEVICE_NUMBER=50 TEST
LOOP=50 DEVICE_NUMBER=20 TEST
```

Then it will serially execute 3 test process with LOOP parameter are 10, 20 and 50, respectively.

> NOTE:
You can change multiple parameters in each test with format like 'LOOP=20 DEVICE_NUMBER=10 TEST', unnecessary space is not allowed. The key word 'TEST' means a new test begins. If you change different parameters, the changed parameters will remain in next tests.

### Start 

After configuring the file 'routine', you can launch the multi-test task by startup script:

```
> ./rep-benchmark.sh
```

Then the test information will show in terminal. 

> NOTE:
If you close the terminal or lose connection to client machine, the test process will terminate. It is the same to any other cases if the output is transmit to terminal.

Using this interface usually takes a long time, you may want to execute the test process as daemon. In this way, you can just launch the test task as daemon by startup script:

```
> ./dae-benchmark.sh
```

In this case, if you want to know what is going on, you can check the log information by command as following:

```
> cd ./logs
> tail -f log_info.log
```

## MySQL Integration

IoTDB-benchmark can automatically store test information into MySQL for further analysis. To enable MySQL Integration, please configure 'config.properties':

```
IS_USE_MYSQL=true
MYSQL_URL=jdbc:mysql://[DB_HOST]:3306/[DBName]?user=[UserName]&password=[PassWord]&useUnicode=true&characterEncoding=UTF8&useSSL=false
```

If you do not need this function, just set ```IS_USE_MYSQL=false``` will be fine.

# Related Article
Benchmark Time Series Database with IoTDB-Benchmark for IoT Scenarios

Arxiv: https://arxiv.org/abs/1901.08304

