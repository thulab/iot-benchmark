# 1. IoTDB-Benchmark
![](https://img.shields.io/badge/platform-MacOS%20%7C%20Linux-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)

You can also read [中文版本](README-cn.md).

# 2. Table of Contents

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
|       OpenTSDB       |    --    |                       Http Request                       |
|       KairosDB       |    --    |                       Http Request                       |
|     TimescaleDB      |    --    |                           jdbc                           |
|        TDengine        |    2.2.0.2    |                           jdbc                           |

# 4. Main Features

IoTDB-benchmark's features are as following:

1. Easy to use: IoTDB-benchmark is a tool combined multiple testing functions so users do not need to switch different tools. 
2. Various data ingestion and testing mode:
   1. Generate periodic time series data according to the configuration and insert and query directly.
   2. Write the generated data to the corresponding location on the disk.
   3. Load data from the generated data set generated in the disk, and write and query.
3. Testing report and result: Supporting storing testing information and results for further query or analysis.
4. Visualize test results: Integration with Tableau to visualize the test result.

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
|       OpenTSDB       |    --    |         opentsdb          |                                                  OpenTSDB                                                   |
|       KariosDB       |    --    |         kairosdb          |                                                  KairosDB                                                   |
|        TDengine        |    2.2.0.2    |          TDengine           |                                                   TDengine                                                    |

# 6. Explanation of different operating modes of IoTDB-Benchmark

This short guide will walk you through the basic process of using IoTDB-benchmark.

## 6.1. Write of Conventional test mode

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
19:09:18.719 [pool-33-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.generate.GenerateBaseClient - pool-4-thread-10 14.60% syntheticWorkload is done.
19:09:18.719 [pool-27-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.generate.GenerateBaseClient - pool-4-thread-8 11.70% syntheticWorkload is done.
19:09:18.719 [pool-42-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.generate.GenerateBaseClient - pool-4-thread-13 7.50% syntheticWorkload is done.
...
```

When test is done, the last output of the test information will be like following: 

```
19:09:21.121 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.BaseMode - All clients finished.
----------------------Main Configurations----------------------
DB_SWITCH: IoTDB-012-SESSION_BY_TABLET
OPERATION_PROPORTION: 1:0:0:0:0:0:0:0:0:0:0
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:1
IS_CLIENT_BIND: true
CLIENT_NUMBER: 20
GROUP_NUMBER: 20
DEVICE_NUMBER: 20
SENSOR_NUMBER: 300
BATCH_SIZE_PER_WRITE: 1
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OUT_OF_ORDER: false
OUT_OF_ORDER_MODE: 0
OUT_OF_ORDER_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 3.62 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           20000               6000000             0                   0                   1659449.63          
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
INGESTION           1.63        0.34        0.62        0.68        0.81        1.03        1.32        1.69        3.82        326.67      548.99      2007.10     
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
+ The latency statistics of different operations in millisecond 
  + ```SLOWEST_THREAD``` is the max accumulative operation time-cost among the client threads

All these information will be logged in ```logs``` directory on client server.

Till now, we have already complete the writing test case without server information recording. If you need to use to complete other tests, please continue reading.

## 6.2. Query of Conventional test mode

In addition to writing data, the conventional test mode can also query data. In addition, this mode also supports mixed read and write operations.

### 6.2.1. Configure

Edit the corresponding parameters in the ```config.properties``` file as following(**Notice**: set ```IS_DELETE_DATA=false```):

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
> Usually the query test is performed after the data ingestion test. Of course, you can also add write operations by modifying ```OPERATION_PROPORTION=INGEST:1:1:1:1:1:1:1:1:1:1```, this parameter will control the inclusion of write and query The ratio of various operations including operations.

### 6.2.2. Start (Without Server System Information Recording)

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
19:11:20.948 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.BaseMode - All clients finished.
----------------------Main Configurations----------------------
DB_SWITCH: IoTDB-012-SESSION_BY_TABLET
OPERATION_PROPORTION: 0:1:1:1:1:1:1:1:1:1:1
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:1
IS_CLIENT_BIND: true
CLIENT_NUMBER: 20
GROUP_NUMBER: 20
DEVICE_NUMBER: 20
SENSOR_NUMBER: 300
BATCH_SIZE_PER_WRITE: 1
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OUT_OF_ORDER: false
OUT_OF_ORDER_MODE: 0
OUT_OF_ORDER_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 10.11 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           0                   0                   0                   0                   0.00                
PRECISE_POINT       2011                1                   0                   0                   0.10                
TIME_RANGE          1983                99152               0                   0                   9806.81             
VALUE_RANGE         1964                98200               0                   0                   9712.65             
AGG_RANGE           2042                2042                0                   0                   201.97              
AGG_VALUE           1912                1912                0                   0                   189.11              
AGG_RANGE_VALUE     1977                1977                0                   0                   195.54              
GROUP_BY            2017                26221               0                   0                   2593.44             
LATEST_POINT        2070                2070                0                   0                   204.74              
RANGE_QUERY_DESC    2056                102800              0                   0                   10167.62            
VALUE_RANGE_QUERY_DESC1968                98400               0                   0                   9732.43             
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT       6.37        0.62        1.81        2.64        4.38        6.97        10.88       16.35       45.62       98.18       114.46      801.26      
TIME_RANGE          8.64        0.74        2.27        3.52        5.96        9.91        14.61       20.41       54.08       244.95      250.24      1349.44     
VALUE_RANGE         2.61        0.59        1.01        1.33        1.78        2.48        4.01        5.54        16.41       70.25       97.60       354.54      
AGG_RANGE           2.80        0.59        0.94        1.24        1.67        2.46        3.97        5.75        19.63       96.81       248.58      555.03      
AGG_VALUE           3.58        0.79        1.21        1.64        2.14        2.95        4.63        6.68        27.94       224.90      249.30      482.86      
AGG_RANGE_VALUE     3.09        0.73        1.13        1.51        2.03        2.87        4.17        5.93        27.14       95.14       114.77      513.89      
GROUP_BY            3.36        0.66        1.04        1.39        1.86        2.71        4.17        5.84        24.28       233.85      254.89      614.39      
LATEST_POINT        4.05        0.29        0.68        0.89        1.30        2.09        3.41        5.31        162.82      191.20      231.06      576.83      
RANGE_QUERY_DESC    8.93        0.97        2.47        3.80        6.18        9.73        14.66       20.19       54.75       247.10      255.70      1255.10     
VALUE_RANGE_QUERY_DESC2.93        0.61        1.01        1.36        1.81        2.58        4.03        5.77        22.42       227.53      236.00      523.96      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

> Note: 
>
> When okOperation is smaller than 1000 or 100, the quantiles P99 and P999 may even bigger than MAX because we use the T-Digest Algorithm which uses interpolation in that scenario. 

## 6.3. Conventional test mode(Using Server System Information Recording)

IoTDB Benchmark allows you to use a database to store system data during the test, and currently supports the use of CSV records.

### 6.3.1. Configure

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

### 6.3.2. Start (With Server System Information Recording)

Before running the test, you need to open the IoTDB service on port 6667.

Then, you can go to `/iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

The test results will be saved in the path ```data``` in CSV format

## 6.4. Test process persistence of Conventional test mode

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

## 6.5. Generate data mode

### 6.5.1. Configure

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
```

> Note:
> The FILE_PATH folder should be an empty folder. If it is not empty, an error will be reported and the generated data set will be stored in this folder.

### 6.5.2. Start

Then, you can go to `/iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.5.3. Execute

Now after launching the test, you will see testing information rolling like following: 

```
...
19:13:58.310 [pool-6-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.generate.GenerateBaseClient - pool-1-thread-5 33.10% syntheticWorkload is done.
19:13:58.316 [pool-3-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.generate.GenerateBaseClient - pool-1-thread-2 68.10% syntheticWorkload is done.
19:13:58.317 [pool-5-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.generate.GenerateBaseClient - pool-1-thread-4 37.20% syntheticWorkload is done.
...
```

When test is done, the last testing information will be like the following: 

```
19:13:59.755 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.GenerateDataMode - Data Location: data/test
19:13:59.755 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.GenerateDataMode - Schema Location: data/test/schema.txt
19:13:59.755 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.GenerateDataMode - Generate Info Location: data/test/info.txt
```

> Note:
> 1. The data storage location is under the FILE_PATH folder, and its directory structure is /d_xxx/batch_xxx.txt
> 2. The metadata of the device and sensor is stored in FILE_PATH/schema.txt
> 3. The relevant information of the data set is stored in FILE_PATH/info.txt

The following is an example of info.txt:

```
LOOP=1000
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
BATCH_SIZE_PER_WRITE=1
START_TIME='2018-9-20T00:00:00+08:00'
IS_OUT_OF_ORDER=false
OUT_OF_ORDER_MODE=0
OUT_OF_ORDER_RATIO=0.5
IS_REGULAR_FREQUENCY=false
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
SENSOR_CODES=[s_0, s_1, s_2, s_3, s_4, s_5, s_6, s_7, s_8, s_9]
```

## 6.6. Write mode of verification
In order to verify the correctness of the data set writing, you can use this mode to write the data set generated in the generated data mode. Currently this mode only supports IoTDB v0.12

### 6.6.1. Configure

For this, you need to modify the following configuration in ```config.properties```:

```
BENCHMARK_WORK_MODE=verificationWriteMode
# Data set storage location
FILE_PATH=data/test
```

Notice:
> 1. The FILE_PATH folder should be the data set generated using the generated data mode
> 2. When running this mode, other parameters should be consistent with the description in info.txt**

### 6.6.2. Start

Before running the test, you need to open the IoTDB service on port 6667.

Then, you can go to `/iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.6.3. Execute
Now after launching the test, you will see testing information rolling like following:

```
...
21:03:06.552 [pool-5-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.real.RealBaseClient - pool-3-thread-1 68.90% realDataWorkload is done.
21:03:06.552 [pool-16-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.real.RealBaseClient - pool-3-thread-5 75.70% realDataWorkload is done.
21:03:06.553 [pool-11-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.real.RealBaseClient - pool-3-thread-3 75.00% realDataWorkload is done.
...
```

When test is done, the last testing information will be like the following:

```
21:03:06.678 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.BaseMode - All clients finished.
----------------------Main Configurations----------------------
DB_SWITCH: IoTDB-012-SESSION_BY_TABLET
OPERATION_PROPORTION: 1:0:0:0:0:0:0:0:0:0:0
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:1
IS_CLIENT_BIND: true
CLIENT_NUMBER: 5
GROUP_NUMBER: 20
DEVICE_NUMBER: 5
SENSOR_NUMBER: 10
BATCH_SIZE_PER_WRITE: 1
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OUT_OF_ORDER: false
OUT_OF_ORDER_MODE: 0
OUT_OF_ORDER_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 1.18 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           5000                50000               0                   0                   42249.45            
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           0.45        0.08        0.10        0.12        0.15        0.25        0.52        0.89        4.10        36.44       70.54       485.69      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 6.7. Query mode of verification

In order to verify the correctness of the data set writing, you can use this mode to query the data set written to the database. Currently this mode only supports IoTDB v0.12

### 6.7.1. Configure

For this, you need to modify the following configuration in ```config.properties```:

```
BENCHMARK_WORK_MODE=verificationQueryMode
# Data set storage location
FILE_PATH=data/test
```

> Note:
> 1. The FILE_PATH folder should be the data set generated using the generated data mode
> 2. When running this mode, other parameters should be consistent with the description in info.txt**

### 6.7.2. Start

Before running the test, you need to open the IoTDB service on port 6667.

Then, you can go to `/iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`, and run the startup script, currently we only support Unix/OS X system: 

```sh
> ./benchmark.sh
```

### 6.7.3. Execute

Now after launching the test, you will see testing information rolling like following: 

```
...
21:05:37.020 [pool-3-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.real.RealBaseClient - pool-1-thread-1 82.80% realDataWorkload is done.
21:05:37.033 [pool-5-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.real.RealBaseClient - pool-1-thread-2 85.00% realDataWorkload is done.
21:05:37.043 [pool-7-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.real.RealBaseClient - pool-1-thread-3 88.80% realDataWorkload is done.
...
```

When test is done, the last testing information will be like the following:

```
----------------------Main Configurations----------------------
DB_SWITCH: IoTDB-012-SESSION_BY_TABLET
OPERATION_PROPORTION: 1:0:0:0:0:0:0:0:0:0:0
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:1
IS_CLIENT_BIND: true
CLIENT_NUMBER: 5
GROUP_NUMBER: 20
DEVICE_NUMBER: 5
SENSOR_NUMBER: 10
BATCH_SIZE_PER_WRITE: 1
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OUT_OF_ORDER: false
OUT_OF_ORDER_MODE: 0
OUT_OF_ORDER_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 12.93 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
VERIFICATION_QUERY  5000                50000               0                   0                   3868.45             
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
VERIFICATION_QUERY  9.84        2.16        3.07        3.71        5.19        8.32        13.62       20.41       42.82       199.66      2242.67     10075.76    
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

# 8. Verification 
1. Now verification only support IoTDB v0.12 and TimescaleDB
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

After configuring the file 'routine', you also need to modify rep-benchmark.sh and dea-benchmark.sh. You need to change cli-benchmark.sh to benchmark.sh

```sh
sh $BENCHMARK_HOME/benchmark.sh
```

before running you can launch the multi-test task by startup script:

```
> ./rep-benchmark.sh
```

Then the test information will show in terminal. 

> NOTE:
If you close the terminal or lose connection to client machine, the test process will terminate. It is the same to any other cases if the output is transmit to terminal.

Using this interface usually takes a long time, you may want to execute the test process as daemon. In this way, you can just launch the test task as daemon by startup script:

```sh
> ./dae-benchmark.sh
```

In this case, if you want to know what is going on, you can check the log information by command as following:

```sh
> cd ./logs
> tail -f log_info.log
```

# 10. Related Article
Benchmark Time Series Database with IoTDB-Benchmark for IoT Scenarios

Arxiv: https://arxiv.org/abs/1901.08304

