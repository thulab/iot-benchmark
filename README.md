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
3. Apache IoTDB v0.8.0 ([https://github.com/apache/incubator-iotdb](https://github.com/apache/incubator-iotdb))
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
2019-10-11 21:55:25,657 INFO  cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper:57 - pool-1-thread-16 insert one batch latency (device: d_8, sg: group_8) ,2.51, ms, throughput ,119593.23947390135, points/s 
2019-10-11 21:55:25,657 INFO  cn.edu.tsinghua.iotdb.benchmark.client.BaseClient:123 - pool-1-thread-16 99.90% syntheticWorkload is done. 
···
```

When test is done, the last two lines of testing information will be like following: 

```
2019-10-11 21:55:25,679 INFO  cn.edu.tsinghua.iotdb.benchmark.App:231 - All clients finished. 
----------------------Test Configurations----------------------
DB_SWITCH: IoTDB
OPERATION_PROPORTION: 2:0:0:0:0:0:0:0:0
IS_CLIENT_BIND: false
CLIENT_NUMBER: 20
GROUP_NUMBER: 20
DEVICE_NUMBER: 20
SENSOR_NUMBER: 300
BATCH_SIZE: 1
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OVERFLOW: false
OVERFLOW_MODE: 0
OVERFLOW_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Test elapse time: 11.07 second
Create schema cost 0.78 second
--------------------------------------------------Result Matrix--------------------------------------------------
Operation               okOperation     okPoint         failOperation   failPoint       elapseRate      accRate
INGESTION               20000           6000000         0               0               542183.26       2830622.39              
PRECISE_POINT           0               0               0               0               0.00            0.00            
TIME_RANGE              0               0               0               0               0.00            0.00            
VALUE_RANGE             0               0               0               0               0.00            0.00            
AGG_RANGE               0               0               0               0               0.00            0.00            
AGG_VALUE               0               0               0               0               0.00            0.00            
AGG_RANGE_VALUE         0               0               0               0               0.00            0.00            
GROUP_BY                0               0               0               0               0.00            0.00            
LATEST_POINT            0               0               0               0               0.00            0.00            
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------Latency (ms) Matrix-----------------------------------------------
Operation       AVG     MID_AVG MIN     P10     P25     MEDIAN  P75     P90     P95     P99     MAX     MAX_SUM 
INGESTION       1.99    1.19    0.55    0.86    0.92    1.06    1.47    2.23    3.03    13.76   299.26  2119.68 
PRECISE_POINT   0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    
TIME_RANGE      0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    
VALUE_RANGE     0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    
AGG_RANGE       0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    
AGG_VALUE       0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    
AGG_RANGE_VALUE 0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    
GROUP_BY        0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    
LATEST_POINT    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    
-----------------------------------------------------------------------------------------------------------------
```

The output contains overall information of the test including:
+ Main configurations
+ Total elapse time during the test
+ Time cost of schema creation
+ okOperation: successfully executed request/SQL number for different operations
+ okPoint: successfully ingested data point number or successfully returned query result point number
+ failOperation: the request/SQL number failed to execute for different operations
+ failPoint: the data point number failed to ingest (for query operations currently this field is always zero)
+ elapseRate: equals to ```okPoint / Test elapse time```
+ accRate(accurate/accumulative rate): equals to ```okPoint / MAX_SUM * 1000```, where ```MAX_SUM``` is the max accumulative operation(database API) time-cost of among the client threads
+ The latency statistics of different operations in millisecond

All these information will be logged in 'iotdb-benchmark/logs' directory on client server.

Till now, we have already complete the writing test case without server information recording. For more advanced usage of IoTDB-benchmark, please follow the 'Other Case' instruction.

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
2019-10-11 23:17:59,314 INFO  cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper:242 - pool-1-thread-7 complete RANGE_QUERY with latency ,9.11, ms ,51, result points 
2019-10-11 23:17:59,314 INFO  cn.edu.tsinghua.iotdb.benchmark.client.BaseClient:123 - pool-1-thread-7 40.00% syntheticWorkload is done. 
2019-10-11 23:17:59,315 INFO  cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb.IoTDB:328 - pool-1-thread-7 query SQL: SELECT s_161 FROM root.group_12.d_12 WHERE time >= 2018-09-20 00:00:15 AND time <= 2018-09-20 00:04:25 AND root.group_12.d_12.s_161 > -5.0 
···
```

When test is done, the last two lines of testing information will be like following: 

```
2019-10-11 23:17:59,472 INFO  cn.edu.tsinghua.iotdb.benchmark.App:231 - All clients finished. 
----------------------Test Configurations----------------------
DB_SWITCH: IoTDB
OPERATION_PROPORTION: 0:1:2:1:1:1:1:1:1
IS_CLIENT_BIND: false
CLIENT_NUMBER: 20
GROUP_NUMBER: 20
DEVICE_NUMBER: 20
SENSOR_NUMBER: 300
BATCH_SIZE: 1
LOOP: 10
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OVERFLOW: false
OVERFLOW_MODE: 0
OVERFLOW_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Test elapse time: 0.43 second
Create schema cost 0.00 second
--------------------------------------------------Result Matrix--------------------------------------------------
Operation               okOperation     okPoint         failOperation   failPoint       elapseRate      accRate
INGESTION               0               0               0               0               0.00            0.00            
PRECISE_POINT           26              26              0               0               61.04           413.49          
TIME_RANGE              27              1377            0               0               3232.57         16822.62                
VALUE_RANGE             18              918             0               0               2155.05         17844.87                
AGG_RANGE               16              16              0               0               37.56           314.56          
AGG_VALUE               16              16              0               0               37.56           267.43          
AGG_RANGE_VALUE         46              46              0               0               107.99          416.17          
GROUP_BY                15              195             0               0               457.77          9267.95         
LATEST_POINT            36              36              0               0               84.51           482.57          
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------Latency (ms) Matrix-----------------------------------------------
Operation       AVG     MID_AVG MIN     P10     P25     MEDIAN  P75     P90     P95     P99     MAX     MAX_SUM 
INGESTION       0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    
PRECISE_POINT   12.97   11.51   1.74    4.33    7.56    10.90   17.01   26.82   27.79   35.16   35.16   62.88   
TIME_RANGE      16.89   15.19   7.15    7.92    9.26    16.09   21.25   31.88   36.90   38.50   38.50   81.85   
VALUE_RANGE     13.95   12.38   2.35    2.48    7.83    13.19   20.24   31.32   31.75   31.75   31.75   51.44   
AGG_RANGE       14.27   11.60   4.44    4.64    8.18    11.98   15.37   34.29   38.88   38.88   38.88   50.86   
AGG_VALUE       13.58   11.05   5.67    7.02    7.91    11.01   16.72   31.99   35.99   35.99   35.99   59.83   
AGG_RANGE_VALUE 37.24   36.29   6.01    7.62    9.96    21.68   67.31   68.00   68.28   68.66   68.66   110.53  
GROUP_BY        10.79   10.50   1.66    7.61    7.68    9.88    14.23   16.28   17.82   17.82   17.82   21.04   
LATEST_POINT    14.32   12.93   2.58    4.72    9.25    13.51   17.69   28.86   34.92   35.32   35.32   74.60   
-----------------------------------------------------------------------------------------------------------------
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

If you do not need this function, just set 'IS_USE_MYSQL=false' will be fine.

#### Related Article
Benchmark Time Series Database with IoTDB-Benchmark for IoT Scenarios

Arxiv: https://arxiv.org/abs/1901.08304

