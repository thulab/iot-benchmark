# Overview

IoTDB-benchmark is a tool for benchmarking IoTDB against other databases and time series solutions.

Current databases supported:

+ IoTDB
+ InfluxDB
+ OpenTSDB
+ KairosDB
+ TimescaleDB
+ CTSDB

# Main Features

IoTDB-benchmark's features are as following:

1. Easy to use. IoTDB-benchmark is a tool combined multiple testing functions so users do not need to switch different tools. 
2. Different writing test model: Load existing data from file; Generate real-time data continuously.
3. Supporting storing testing information and results for further query or analysis.
4. Integration with Jenkins to report test result automatically.

# Prerequisites

To use IoTDB-benchmark, you need to have:

1. Java >= 1.8
2. Maven >= 3.0 (If you want to compile and install IoTDB from source code)
3. TsFile >= 0.2.0 (TsFile Github page: [https://github.com/thulab/tsfile](https://github.com/thulab/tsfile))
4. IoTDB-JDBC >= 0.1.2 (IoTDB-JDBC Github page: [https://github.com/thulab/iotdb-jdbc](https://github.com/thulab/iotdb-jdbc))
5. IoTDB >= 0.0.1 (IoTDB Github page: [https://github.com/thulab/iotdb](https://github.com/thulab/iotdb))
6. InfluxDB >= 1.3.7
7. influxdb-comparisons (If you want to use the load-data-from-file mode)
8. sysstat (If you want to record system information of DB-server during test)
9. other database system under test

# Quick Start

This short guide will walk you through the basic process of using IoTDB-benchmark.

## Build

You can build IoTDB-benchmark using Maven:

```
mvn clean package -Dmaven.test.skip=true
```

> NOTE:
In current version we re-build the project in every lauching process due to debug requirements so this command is in the launching-test scripts like 'cli-benchmark.sh'. You can just directly start the script. We will delete that when the functions are stable.

## Configure

Before starting any new test case, you need to config the configuration files 'config.properties' first which is in 'iotdb-benchmark/conf'. For your convenience, we have already set the default config for the following demonstration.

Suppose you are going to test writing performance of IoTDB and you do not care about the system information of the IoTDB server. You have installed IoTDB in a IoTDB server with default settings and the IP of this server is 192.168.130.9. You want to test it under the parameter setting as following:

```
+--------------------------+------------+--------------+-------------+------------+-------------------+------+
| Measurement/StorageGroup | tag/device | field/sensor | concurrency | batch size | point interval(s) | loop |
+--------------------------+------------+--------------+-------------+------------+-------------------+------+
|            10            |    100     |      100     |     10      |      1     |         5         | 1000 |
+--------------------------+------------+--------------+-------------+------------+-------------------+------+
```

Then you are supposed to edit the 'config.properties' file as following:

```
HOST=192.168.130.9
PORT=6667
DB_SWITCH=IoTDB
SERVER_MODE=false
IS_GEN_DATA=false
GROUP_NUMBER = 10
DEVICE_NUMBER=100
SENSOR_NUMBER=100
CLIENT_NUMBER= 10
CACHE_NUM=1
POINT_STEP = 5000
LOOP=1000
```

> NOTE:
Other irrelevant parameters are omitted. You can just set as default. We will cover them later in other cases.

## Start (Without Server System Information Recording)

Running the startup script, currently we only support Unix/OS X system: 

```
> ./benchmark.sh
```

## Execute 

Now after launching the test, you will see testing information rolling like following: 

```
···
2017-11-16 17:38:46,115 INFO  cn.edu.tsinghua.iotdb.benchmark.db.iotdb.IoTDB:196 - pool-1-thread-2 execute 0 loop, it costs 0.032s, totalTime1.976, throughput 468750.0 points/s 
2017-11-16 17:38:46,118 INFO  cn.edu.tsinghua.iotdb.benchmark.db.iotdb.IoTDB:196 - pool-1-thread-1 execute 0 loop, it costs 0.032s, totalTime1.836, throughput 468750.0 points/s 
···
```

Each line contains information about every batch test, including:
+ client thread index 
+ loop index
+ batch test time cost
+ corresponding client thread current total time cost
+ throughput during this batch

When test is done, the last two lines of testing information will be like following: 

```
2017-11-18 16:04:19,997 INFO  cn.edu.tsinghua.iotdb.benchmark.App:142 - loaded ,1000000000, points in ,2201.669,s with ,10, workers (mean rate ,454200.88, points/s) 
2017-11-18 16:04:19,998 INFO  cn.edu.tsinghua.iotdb.benchmark.App:150 - total error num is ,0, create schema cost ,6.643,s
```

These two lines contain overall information of this test case, including:
+ total points inserted
+ insertion excuting time cost
+ client thread number
+ mean throughput
+ total error number
+ creating schema time cost

All these information will be logged in 'iotdb-benchmark/logs' directory on client server.

Till now, we have already complete the writing test case without server information recording. For more advanced usage of IoTDB-benchmark, please follow the 'Other Case' instruction.

# Other Cases

## Test IoTDB With Server System Information Recording

### Configure

If you are using IoTDB-benchmark for the first time, you neet to config testing related environment in startup scripts 'cli-benchmark'.
Suppose your IoTDB server IP is 192.168.130.9 and your test client server which installed IoTDB-benchmark has authorized ssh access to the IoTDB server.
Current version of information recording is dependent on iostat. Please make sure iostat is installed in IoTDB server.

Configure 'config.properties'
Suppose you are using the same parameters as in Quick Start case. The new parameters you should add are INTERVAL and LOG_STOP_FLAG_PATH like:

```
INTERVAL=0
LOG_STOP_FLAG_PATH=/home/liurui
```

INTERVAL=0 means the server information recording with the minimal interval 2 seconds. If you set INTERVAL=n then the interval will be n+2 seconds since the recording process require least 2 seconds. You may want to set the INTERVAL longer when conducting long testing.
LOG_STOP_FLAG_PATH is the directory where to touch the file 'log_stop_flag' to stop the recording process. Therefore it has to be accessible by IoTDB-benchmark. It is also the data directory of IoTDB.

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
The benchmark will automatically initial database, there is no need for executing SQL like "drop database test".

### Start (With Server System Information Recording)

After configuring the first-time test environment, you can just launch the test by startup script:

```
> ./cli-benchmark.sh
```

Then one test process is on going.

## Multiple Tests Comparisons

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

## Sample Data Generation

If you want to generate and insert customized sample timeseries data into IoTDB(we may support other database later). And write the corresponding insertion SQL into a file at the same time. Please follow instructions of this section. We will show you an example.

Suppose you want generate a sample dataset as following:

```
+-------------------------------+-----------+-----------+---------------+---------------+
|          timeseries           |    type   |   encode  |     scope     | storage group | 
+-------------------------------+-----------+-----------+---------------+---------------+
|    root.ln.wf01.wt01.status   |  BOOLEAN  |   PLAIN   |   true,false  |    root.ln    | 
+-------------------------------+-----------+-----------+---------------+---------------+
| root.ln.wf01.wt01.temperature |   INT32   |    RLE    |     13,16     |    root.ln    |    
+-------------------------------+-----------+-----------+---------------+---------------+
|     root.ln.wf01.wt01.time    |   FLOAT   |    RLE    | 5600.0,6200.0 |    root.ln    | 
+-------------------------------+-----------+-----------+---------------+---------------+
|   root.ln.wf02.wt02.hardware  |   TEXT    |   PLAIN   |     v1,v2     |    root.ln    |  
+-------------------------------+-----------+-----------+---------------+---------------+
```

data point frequency: 1 minite 
time range of data point: 2017/11/1T00:00:00 - 2017/11/7T24:00:00

### Configure

Configure 'config.properties'
The other writing related parameters are the same way in previous cases, the parameters related are:

```
HOST=127.0.0.1
PORT=6667
DB_SWITCH=IoTDB
CLIENT_NUMBER=1
CACHE_NUM=10
LOOP=1008
POINT_STEP=60000
IS_GEN_DATA=true
GEN_DATA_FILE_PATH=/home/parallels/sampleData
```

Configure 'routine'
For the case above, you need to configure the 'routine' file as:

```
IS_GEN_DATA=true
STORAGE_GROUP_NAME=root.ln
TIMESERIES_NAME=wf01.wt01.status
TIMESERIES_TYPE=BOOLEAN
ENCODING=PLAIN
TIMESERIES_VALUE_SCOPE=true,false
TEST
TIMESERIES_NAME=wf01.wt01.temperature
TIMESERIES_TYPE=INT32
ENCODING=RLE
TIMESERIES_VALUE_SCOPE=13,16
TEST
TIMESERIES_NAME=wf01.wt01.time
TIMESERIES_TYPE=FLOAT
ENCODING=RLE
TIMESERIES_VALUE_SCOPE=5600.0,6200.0
TEST
TIMESERIES_NAME=wf02.wt02.hardware
TIMESERIES_TYPE=TEXT
ENCODING=PLAIN
TIMESERIES_VALUE_SCOPE=v1,v2
TEST
```

> NOTE:
If 'TIMESERIES_TYPE' is 'BOOLEAN', the parameter 'TIMESERIES_VALUE_SCOPE' is optional. 
If 'TIMESERIES_TYPE' is 'INT32' or 'FLOAT', the parameter 'TIMESERIES_VALUE_SCOPE' can contain only one range. 
If 'TIMESERIES_TYPE' is 'TEXT', the parameter 'TIMESERIES_VALUE_SCOPE' can contain several different enum values devided by ',' like 'TIMESERIES_VALUE_SCOPE=apple,egg,orange,cake'. 

### Start 

After configuring the file 'routine', you can launch the sample data generation task by startup script:

```
> ./rep-benchmark.sh
```

Then the sample timeseries will be generated one by one in order of the sequence in routine. 


## MySQL Integration

IoTDB-benchmark can automatically store test information into MySQL for further analysis. To enable MySQL Integration, please configure 'config.properties':

```
IS_USE_MYSQL=true
MYSQL_URL=jdbc:mysql://[DB_HOST]:3306/[DBName]?user=[UserName]&password=[PassWord]&useUnicode=true&characterEncoding=UTF8&useSSL=false
```

If you do not need this function, just set 'IS_USE_MYSQL=false' will be fine.
