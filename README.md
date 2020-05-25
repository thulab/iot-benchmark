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
- [The relation of IoTDB and Benchmark with version](#The relation of IoTDB and Benchmark with version)
		- [IoTDB 0.10.0](#When the version of IoTDB is master(0.10.0) , you have to use the version of dev of benchmark to test it.)
		- [IoTDB 0.9.0](#When the version of IoTDB is not master(0.9.0) , you have to use the version of master of benchmark to test it.)
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
	- [Test Data Persistence](#test-data-persistence)
- [Related Article](#related-article)

<!-- /MarkdownTOC -->

# Overview

IoTDB-benchmark is a tool for benchmarking IoTDB against other databases and time series solutions.

Databases currently supported:

+ IoTDB
+ InfluxDB
+ KairosDB
+ TimescaleDB
+ OpenTSDB

# Main Features

IoTDB-benchmark's features are as following:

1. Easy to use. IoTDB-benchmark is a tool combined multiple testing functions so users do not need to switch different tools. 
2. Various data ingestion and testing mode: Load existing data from file; Generate periodical time series data, etc.
3. Supporting storing testing information and results for further query or analysis.
4. Integration with Tableau to visualize the test result.

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

Currently, we have multiple ingestion modes for IoTDB v0.9.x. Specifically, jdbc mode(ingestion using jdbc) and session mode(ingestion using session).
You can specify it in the following parameter of the ```conf/config.properties``` file.
```
INSERT_MODE=session
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
2019-11-12 14:48:06,195 INFO  cn.edu.tsinghua.iotdb.benchmark.App:230 - All clients finished. 
----------------------Main Configurations----------------------
DB_SWITCH: IoTDB
OPERATION_PROPORTION: 1:0:0:0:0:0:0:0:0
IS_CLIENT_BIND: true
CLIENT_NUMBER: 20
GROUP_NUMBER: 20
DEVICE_NUMBER: 20
SENSOR_NUMBER: 300
BATCH_SIZE: 100
LOOP: 100
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OVERFLOW: false
OVERFLOW_MODE: 0
OVERFLOW_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 1.72 second
Test elapsed time (not include schema creation): 4.76 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           2000                60000000            0                   0                   12595552.69         
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
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           41.29       3.63        5.13        6.18        10.54       32.96       120.54      176.69      503.08      562.68      597.85      4409.70     
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
>
> Usually the query test is performed after the data ingestion test. Of course you can add ingestion operation at the same time by setting ```OPERATION_PROPORTION=INGEST:1:2:1:1:1:1:1:1``` as long as ```INGEST``` is not zero, since the parameter ```OPERATION_PROPORTION``` is to control the proportion of different operations including ingestion and query operations.


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
2019-11-12 14:49:16,824 INFO  cn.edu.tsinghua.iotdb.benchmark.App:230 - All clients finished. 
----------------------Main Configurations----------------------
DB_SWITCH: IoTDB
OPERATION_PROPORTION: 0:1:2:1:1:1:1:1:1
IS_CLIENT_BIND: true
CLIENT_NUMBER: 20
GROUP_NUMBER: 20
DEVICE_NUMBER: 20
SENSOR_NUMBER: 300
BATCH_SIZE: 100
LOOP: 100
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OVERFLOW: false
OVERFLOW_MODE: 0
OVERFLOW_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 2.67 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           0                   0                   0                   0                   0.00                
PRECISE_POINT       236                 236                 0                   0                   88.29               
TIME_RANGE          438                 22338               0                   0                   8356.46             
VALUE_RANGE         221                 11271               0                   0                   4216.39             
AGG_RANGE           213                 213                 0                   0                   79.68               
AGG_VALUE           205                 205                 0                   0                   76.69               
AGG_RANGE_VALUE     240                 240                 0                   0                   89.78               
GROUP_BY            213                 2769                0                   0                   1035.86             
LATEST_POINT        234                 234                 0                   0                   87.54               
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT       15.05       2.20        3.56        4.45        6.69        12.24       29.59       72.03       121.50      173.74      168.56      370.39      
TIME_RANGE          16.04       2.05        3.30        4.42        7.04        14.10       38.82       69.68       130.26      173.65      169.96      597.47      
VALUE_RANGE         29.65       3.04        6.27        8.31        12.05       29.91       87.89       108.73      145.34      289.35      275.96      763.06      
AGG_RANGE           14.69       2.05        3.48        4.62        7.08        14.30       34.46       53.81       88.67       154.63      148.23      334.28      
AGG_VALUE           51.49       5.46        8.44        11.68       25.90       84.37       126.33      152.03      178.08      370.07      340.11      811.48      
AGG_RANGE_VALUE     51.46       3.52        5.80        7.87        13.20       46.99       157.19      245.99      360.70      377.33      374.94      847.19      
GROUP_BY            14.30       2.35        3.15        4.33        7.09        15.20       33.65       52.24       80.08       137.65      129.35      276.62      
LATEST_POINT        14.03       1.81        2.49        3.46        5.84        12.46       36.78       57.48       98.36       146.52      139.68      397.61      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

> Note: 
>
> When okOperation is smaller than 1000 or 100, the quantiles P99 and P999 may even bigger than MAX because we use the T-Digest Algorithm which uses interpolation in that scenario. 

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

## Test Data Persistence

IoTDB-benchmark can automatically store test information into database for further analysis. 
 
To enable MySQL Integration, please configure ```config.properties```:

```
TEST_DATA_PERSISTENCE=MySQL
TEST_DATA_STORE_IP=166.111.7.145
TEST_DATA_STORE_PORT=3306
TEST_DATA_STORE_DB=test
TEST_DATA_STORE_USER=root
TEST_DATA_STORE_PW=root
```

If you do not want to store test data, set ```TEST_DATA_PERSISTENCE=None```.

# Related Article
Benchmark Time Series Database with IoTDB-Benchmark for IoT Scenarios

Arxiv: https://arxiv.org/abs/1901.08304

