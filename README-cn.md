# 1. iot-benchmark
![](https://img.shields.io/badge/platform-MacOS%20%7C%20Linux%20%7C%20Windows-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)

# 2. 内容目录

<!-- TOC -->

- [1. iot-benchmark](#1-iot-benchmark)
- [2. 内容目录](#2-内容目录)
- [3. 概述](#3-概述)
- [4. 主要特点](#4-主要特点)
- [5. iot-benchmark的使用](#5-iot-benchmark的使用)
  - [5.1. iot-benchmark运行的前置条件](#51-iot-benchmark运行的前置条件)
  - [5.2. iot-benchmark支持的运行模式](#52-iot-benchmark支持的运行模式)
  - [5.3. iot-benchmark的编译构建](#53-iot-benchmark的编译构建)
- [6. iot-benchmark的不同运行模式的说明](#6-iot-benchmark的不同运行模式的说明)
  - [6.1. 常规测试模式之写入(单数据库)](#61-常规测试模式之写入单数据库)
    - [6.1.1. Benchmark的配置](#611-benchmark的配置)
    - [6.1.2. Benchmark的启动](#612-benchmark的启动)
    - [6.1.3. Benchmark的执行](#613-benchmark的执行)
  - [6.2. 常规测试模式之查询(单数据库，不使用系统记录)](#62-常规测试模式之查询单数据库不使用系统记录)
    - [6.2.1. Benchmark的配置](#621-benchmark的配置)
    - [6.2.2. Benchmark的启动](#622-benchmark的启动)
    - [6.2.3. Benchmark的执行](#623-benchmark的执行)
  - [6.3. 常规测试模式之读写混合模式（单数据库）](#63-常规测试模式之读写混合模式单数据库)
    - [6.3.1. Benchmark的配置](#631-benchmark的配置)
    - [6.3.2. Benchmark的启动](#632-benchmark的启动)
    - [6.3.3. Benchmark的执行](#633-benchmark的执行)
  - [6.4. 常规测试模式之读写混合模式（单数据库，查询最近写入数据）](#64-常规测试模式之读写混合模式单数据库查询最近写入数据)
    - [6.4.1. Benchmark的配置](#641-benchmark的配置)
    - [6.4.2. Benchmark的启动](#642-benchmark的启动)
    - [6.4.3. Benchmark的执行](#643-benchmark的执行)
  - [6.5. 常规测试模式之使用系统记录（单数据库）](#65-常规测试模式之使用系统记录单数据库)
    - [6.5.1. Benchmark的配置](#651-benchmark的配置)
    - [6.5.2. Benchmark的启动](#652-benchmark的启动)
  - [6.6. 常规测试模式之测试过程持久化（单数据库）](#66-常规测试模式之测试过程持久化单数据库)
  - [6.7. 生成数据模式](#67-生成数据模式)
    - [6.7.1. Benchmark的配置](#671-benchmark的配置)
    - [6.7.2. Benchmark的启动](#672-benchmark的启动)
    - [6.7.3. Benchmark的执行](#673-benchmark的执行)
  - [6.8. 正确性写入模式（单数据库，外部数据集）](#68-正确性写入模式单数据库外部数据集)
    - [6.8.1. Benchmark的配置](#681-benchmark的配置)
    - [6.8.2. Benchmark的启动](#682-benchmark的启动)
    - [6.8.3. Benchmark的执行](#683-benchmark的执行)
  - [6.9. 正确性单点查询模式（单数据库，外部数据集）](#69-正确性单点查询模式单数据库外部数据集)
    - [6.9.1. Benchmark的配置](#691-benchmark的配置)
    - [6.9.2. Benchmark的启动](#692-benchmark的启动)
    - [6.9.3. Benchmark的执行](#693-benchmark的执行)
  - [6.10. 双数据库模式](#610-双数据库模式)
  - [6.11. 常规测试模式之写入(双数据库)](#611-常规测试模式之写入双数据库)
    - [6.11.1. Benchmark的配置](#6111-benchmark的配置)
    - [6.11.2. Benchmark的启动](#6112-benchmark的启动)
    - [6.11.3. Benchmark的执行](#6113-benchmark的执行)
  - [6.12. 正确性单点查询模式（双数据库比较）](#612-正确性单点查询模式双数据库比较)
    - [6.12.1. Benchmark的配置](#6121-benchmark的配置)
    - [6.12.2. Benchmark的启动](#6122-benchmark的启动)
    - [6.12.3. Benchmark的执行](#6123-benchmark的执行)
  - [6.13. 正确性功能查询模式（双数据库比较）](#613-正确性功能查询模式双数据库比较)
    - [6.13.1. Benchmark的配置](#6131-benchmark的配置)
    - [6.13.2. Benchmark的启动](#6132-benchmark的启动)
    - [6.13.3. Benchmark的执行](#6133-benchmark的执行)
- [7. 使用IoTDB Benchmark测试其他数据库(部分)](#7-使用iotdb-benchmark测试其他数据库部分)
  - [7.1. 测试 InfluxDB v1.x](#71-测试-influxdb-v1x)
  - [7.2. 测试 InfluxDB v2.0](#72-测试-influxdb-v20)
  - [7.3. 测试 Microsoft SQL Server](#73-测试-microsoft-sql-server)
  - [7.4. 测试 QuestDB](#74-测试-questdb)
  - [7.5. 测试 SQLite](#75-测试-sqlite)
  - [7.6. 测试 Victoriametrics](#76-测试-victoriametrics)
  - [7.7. 测试 TimeScaleDB](#77-测试-timescaledb)
  - [7.8. 测试 PI Archive](#78-测试-pi-archive)
  - [7.9. 测试 TDengine](#79-测试-tdengine)
- [8. 正确性验证的进一步说明](#8-正确性验证的进一步说明)
- [9. 自动化脚本](#9-自动化脚本)
  - [9.1. 一键化启动脚本](#91-一键化启动脚本)
  - [9.2. 自动执行多项测试](#92-自动执行多项测试)
    - [9.2.1. 配置 routine](#921-配置-routine)
    - [9.2.2. 开始测试](#922-开始测试)
- [11. 相关文章](#11-相关文章)

<!-- /TOC -->

# 3. 概述

IoT-Benchmark 是用来评估时序数据库、实时数据库在工业物联网（IIoT）场景下的性能的基准测试工具。

目前支持如下数据库、版本和连接方式：

|        数据库        |   版本   |                         连接方式                         |
| :------------------: | :------: | :------------------------------------------------------: |
|        IoTDB         |   v1.x   | jdbc、sessionByTablet、sessionByRecord、sessionByRecords |
|       InfluxDB       |   v1.x   |                           SDK                            |
|       InfluxDB       |   v2.0   |                           SDK                            |
|       QuestDB        |  v6.0.7  |                           jdbc                           |
| Microsoft SQL Server | 2016 SP2 |                           jdbc                           |
|   VictoriaMetrics    | v1.64.0  |                       Http Request                       |
|        SQLite        |    --    |                           jdbc                           |
|       OpenTSDB       |    --    |                       Http Request                       |
|       KairosDB       |    --    |                       Http Request                       |
|     TimescaleDB      |    --    |                           jdbc                           |
|     TimescaleDB      | Cluster  |                           jdbc                           |
|       TDengine       | 2.2.0.2  |                           jdbc                           |
|       TDengine       |  3.0.1   |                           jdbc                           |
|      PI Archive      |   2016   |                           jdbc                           |


# 4. 主要特点

iot-benchmark的特点如下：

1. 使用方便：iot-benchmark是一个结合了多种测试功能的工具，用户不需要切换不同的工具。
2. 多种数据插入和测试模式：
   1. 按照配置生成周期性的时间序列数据并直接插入和查询。
   2. 将生成的数据写入到磁盘中对应位置。
   3. 从磁盘中生成的生成的数据集加载数据，并写入和查询。
   4. 对数据和查询结果分别进行正确性验证测试。
3. 测试报告与结果：支持存储测试信息和结果以供进一步查询或分析。
4. 可视化测试结果：与Tableau集成以可视化测试结果。

# 5. iot-benchmark的使用

## 5.1. iot-benchmark运行的前置条件

为了使用iot-benchmark，你需要拥有：

1. Java 8
2. Maven：不建议使用镜像源，国内可以使用阿里云镜像源。
3. 合适版本的数据库
   1. Apache IoTDB >= v1.0([获取方式](https://github.com/apache/iotdb))
   2. 其他的对应版本的数据库
4. CSV的记录模式只能在Linux系统中使用，记录测试过程中的相关系统信息。
5. 我们建议使用MacOs或Linux系统，本文以MacOS和Linux系统为例，如果使用Windows系统，请使用`conf`文件夹下的`benchmark.bat`脚本启动benchmark。

## 5.2. iot-benchmark支持的运行模式
|    模式名称    |  BENCHMARK_WORK_MODE  | 模式内容                                                                         |
| :------------: | :-------------------: | :------------------------------------------------------------------------------- |
|  常规测试模式  |  testWithDefaultPath  | 支持多种读和写操作的混合负载                                                     |
|  生成数据模式  |   generateDataMode    | Benchmark生成数据集到FILE_PATH路径中                                             |
| 正确性写入模式 | verificationWriteMode | 从FILE_PATH路径中加载数据集进行写入，目前支持 IoTDB v1.0 及更新的版本           |
| 正确性查询模式 | verificationQueryMode | 从FILE_PATH路径中加载数据集和数据库中进行比对，目前支持 IoTDB v1.0 及更新的版本 |

## 5.3. iot-benchmark的编译构建

你可以使用Maven完成iot-benchmark的构建，在项目**根目录**中使用如下命令：

```sh
mvn clean package -Dmaven.test.skip=true
```

该命令会编译iot-benchmark的core模块，和所有其他相关的数据库。

在完成编译后，以IoTDB v1.0为例，你可以进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`文件夹下，使用`./benchmark.sh`来启动对IoTDB v1.0的测试。

默认的配置文件存放在`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0/conf`下，您可以编辑`config.properties`来完成配置，请**注意的是，您需要将配置文件中的DB_SWITCH参数调整为您需要被测数据库**，其对应关系和可能取值如下所示：

|        数据库        |   版本   |     对应子项目      |                                                  DB_SWITCH                                                   |
| :------------------: | :------: | :-----------------: | :----------------------------------------------------------------------------------------------------------: |
|        IoTDB         |   1.3    |      iotdb-1.3      | IoTDB-130-JDBC<br>IoTDB-130-SESSION_BY_TABLET<br>IoTDB-130-SESSION_BY_RECORD<br>IoTDB-130-SESSION_BY_RECORDS |
|        IoTDB         |   1.1    |      iotdb-1.1      | IoTDB-110-JDBC<br>IoTDB-110-SESSION_BY_TABLET<br>IoTDB-110-SESSION_BY_RECORD<br>IoTDB-110-SESSION_BY_RECORDS |
|        IoTDB         |   1.0    |      iotdb-1.0      | IoTDB-100-JDBC<br>IoTDB-100-SESSION_BY_TABLET<br>IoTDB-100-SESSION_BY_RECORD<br>IoTDB-100-SESSION_BY_RECORDS |
|       InfluxDB       |   v1.x   |      influxdb       |                                                   InfluxDB                                                   |
|       InfluxDB       |   v2.0   |    influxdb-2.0     |                                                 InfluxDB-2.0                                                 |
|       QuestDB        |  v6.0.7  |       questdb       |                                                   QuestDB                                                    |
| Microsoft SQL Server | 2016 SP2 |     mssqlserver     |                                                 MSSQLSERVER                                                  |
|   VictoriaMetrics    | v1.64.0  |   victoriametrics   |                                               VictoriaMetrics                                                |
|     TimescaleDB      |          |     timescaledb     |                                                 TimescaleDB                                                  |
|     TimescaleDB      | Cluster  | timescaledb-cluster |                                             TimescaleDB-Cluster                                              |
|        SQLite        |    --    |       sqlite        |                                                    SQLite                                                    |
|       OpenTSDB       |    --    |      opentsdb       |                                                   OpenTSDB                                                   |
|       KairosDB       |    --    |      kairosdb       |                                                   KairosDB                                                   |
|       TDengine       | 2.2.0.2  |      TDengine       |                                                   TDengine                                                   |
|       TDengine       |  3.0.1   |      TDengine       |                                                  TDengine-3                                                  |
|      PI Archive      |   2016   |      PIArchive      |                                                  PIArchive                                                   |

# 6. iot-benchmark的不同运行模式的说明
以下所有测试均在如下环境中进行：

```
CPU：I7-11700
内存：32G DDR4
系统盘：512G SSD (INTEL SSDPEKNU512GZ)
数据盘：2T HDD (WDC WD40EZAZ-00SF3B0)
```

## 6.1. 常规测试模式之写入(单数据库)

这个简单的指引将以常规测试模式为例带你快速熟悉iot-benchmark的使用基本流程。

### 6.1.1. Benchmark的配置

在你开始任何一个新的测试用例前，你都需要确认配置文件```config.properties```中的配置信息。为了您的方便，我们已经为以下演示设置了默认配置。

假设您要测试 IoTDB 的数据库性能。您已经安装了IoTDB依赖项并使用默认配置启动了IoTDB服务器。服务器的IP是127.0.0.1。假设工作负载参数是：

```
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
| Measurement/StorageGroup | tag/device | field/sensor | concurrency | batch size | point interval(ms) |  loop  |
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
|            10            |    50      |      500     |     20      |    100     |        200         |  10000 |
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
```

注意：此配置下总的时间序列数为：```deivce * sensor = 25,000```，每个时间序列的点数为 ```batch size * loop = 20,000```，
总数据点数为 ```deivce * sensor * batch size * loop = 500,000,000```。 每个数据点占用空间大小可以按 16 字节估计，则原始数据总大小为 8G。

那么对应的，需要修改```config.properties```文件如下所示：

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

当然，你也可以其他参数，更多的配置参见[config.properties](configuration/conf/config.properties)。

### 6.1.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.1.3. Benchmark的执行

测试启动后，你可以看到滚动的测试执行信息，其中部分信息如下：

```
...
2022-05-08 14:26:36,478 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-4 17.10% workload is done. 
2022-05-08 14:26:41,479 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-13 56.59% workload is done. 
2022-05-08 14:26:41,479 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-2 18.01% workload is done. 
2022-05-08 14:26:41,480 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-14 54.01% workload is done. 
...
```

当测试结束后，最后会显示出本次测试的统计信息，如下所示：

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

输出包含测试的整体信息，包括：
+ 主要的配置信息
+ 测试的总用时 Test elapsed time
+ 元数据创建的总用时
+ 结果矩阵
  + okOperation：执行成功的不同操作的Request/SQL的数量
  + okPoint: 插入成功的数据点数或成功返回查询结果的数据点数
  + failOperation: 执行失败的不同操作的Request/SQL的数量
  + failPoint: 插入失败的数据点数(对于查询该值总为0)
  + throughput: 等于```okPoint / Test elapsed time```
  + <a href = "https://y8dp9fjm8f.feishu.cn/file/boxcndtRvCh3qRNScNm8J5XERWf">参数详细说明</a>
+ 不同操作的毫秒级延迟统计
  + 其中```SLOWEST_THREAD``` 是客户端线程中的最大的累积操作时间长度

以上的全部信息都会被记录到运行设备的```logs```文件夹中。

如果您想要使用乱序方式写入数据，那么您需要对`config.properties`文件的如下属性进行修改：

```
# 是否乱序写入
IS_OUT_OF_ORDER=true
# 乱序写入模式，目前如下2种
# POISSON 按泊松分布的乱序模式
# BATCH 批插入乱序模式
OUT_OF_ORDER_MODE=BATCH
# 乱序写入的数据的比例
OUT_OF_ORDER_RATIO=0.5
# 是否为等长时间戳
IS_REGULAR_FREQUENCY=true
# 泊松分布的期望和方差
LAMBDA=2200.0
# 泊松分布模型的随机数的最大值
MAX_K=170000
```

直到现在，我们已经完成了常规测试模式的写入测试。如果需要使用完成其他测试请继续阅读。

## 6.2. 常规测试模式之查询(单数据库，不使用系统记录)

常规测试模式除了用于写入数据，还可以仅仅查询数据。

### 6.2.1. Benchmark的配置

修改```config.properties```文件中的相关参数如下(其中格外注意设置```IS_DELETE_DATA=false```，来关闭数据清理)：

```properties
### Main Data Ingestion and Query Shared Parameters
HOST=127.0.0.1
PORT=6667
IS_DELETE_DATA=false
DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

> 注意：
> `config.properties`中包含了查询相关的配置参数，您可以通过查看示例文件来了解。

### 6.2.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.2.3. Benchmark的执行

测试启动后，你可以看到滚动的测试执行信息，其中部分信息如下：

```
...
2022-05-08 14:55:37,228 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-14 93.37% workload is done. 
2022-05-08 14:55:37,228 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-17 94.40% workload is done. 
2022-05-08 14:55:37,228 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-8 99.43% workload is done. 
2022-05-08 14:55:37,228 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-18 97.40% workload is done. 
...
```

当测试结束后，最后会显示出本次测试的统计信息，如下所示：

```
2022-05-08 14:55:47,915 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
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

> 注意：
> 当 okOperation 小于 1000 或 100 时，因为我们使用 T-Digest 算法，分位数 P99 和 P999 甚至可能大于 MAX（该算法在该场景中使用插值）。

## 6.3. 常规测试模式之读写混合模式（单数据库）

常规测试模式可以支持用户进行读写混合的测试，需要注意的是这种场景下的读写混合的时间戳都是从**写入开始时间**开始。

### 6.3.1. Benchmark的配置

修改```config.properties```文件中的相关参数如下(其中格外注意设置```IS_RECENT_QUERY=false```，来关闭最近查询模式)：

```properties
### Main Data Ingestion and Query Shared Parameters
HOST=127.0.0.1
PORT=6667
IS_DELETE_DATA=false
DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

### 6.3.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.3.3. Benchmark的执行

测试启动后，你可以看到滚动的测试执行信息，其中部分信息如下：

```
...
2022-05-08 15:00:23,000 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-10 39.63% workload is done. 
2022-05-08 15:00:23,000 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-1 39.26% workload is done. 
2022-05-08 15:00:23,000 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-19 43.91% workload is done. 
2022-05-08 15:00:23,000 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-16 45.84% workload is done. 
...
```

当测试结束后，最后会显示出本次测试的统计信息，如下所示：

```
2022-05-08 15:02:03,959 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
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


## 6.4. 常规测试模式之读写混合模式（单数据库，查询最近写入数据）
常规测试模式可以支持用户进行读写混合的测试(查询最近写入数据），需要注意的是这种场景下的查询时间范围为当前最大写入时间戳的左侧临近的数据。

### 6.4.1. Benchmark的配置

修改```config.properties```文件中的相关参数如下(其中格外注意设置```IS_RECENT_QUERY=true```，来关闭最近查询模式)：

```properties
### Main Data Ingestion and Query Shared Parameters
HOST=127.0.0.1
PORT=6667
IS_DELETE_DATA=false
DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

### 6.4.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.4.3. Benchmark的执行

测试启动后，你可以看到滚动的测试执行信息，其中部分信息如下：

```
...
2022-05-08 15:06:34,593 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
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

## 6.5. 常规测试模式之使用系统记录（单数据库）

IoTDB Benchmark支持您使用数据库存储测试过程中的系统数据，目前支持使用CSV记录。

### 6.5.1. Benchmark的配置

假设您的 IoTDB 服务器 IP 是 192.168.130.9，并且您安装了 iot-benchmark 的测试客户端服务器已授权访问 IoTDB 服务器。

当前版本的信息记录依赖于 iostat。请确保 iostat 已安装在 IoTDB 服务器中。

之后配置```config.properties```
假设您使用的参数与[简单指引](#61-常规测试模式之写入单数据库)中的参数相同。您应该添加的新参数是 TEST_DATA_PERSISTENCE 和 MONITOR_INTERVAL，例如：

```properties
TEST_DATA_PERSISTENCE=CSV
MONITOR_INTERVAL=0
```

> 1. TEST_DATA_PERSISTENCE=CSV 表示测试结果保存到CSV中。
> 2. INTERVAL=0 表示服务器信息记录的间隔最小为 2 秒。 如果您设置 INTERVAL=n，那么间隔将为 n+2 秒，因为记录过程至少需要2秒。在进行长时间测试时，您可能希望将 INTERVAL 设置得更长。

### 6.5.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

其他后续过程和上文所述类似，最后生成的相关文件会存放在```data```目录下。

## 6.6. 常规测试模式之测试过程持久化（单数据库）

为了后续的分析，iot-benchmark可以将测试信息存储到数据库中(如果你不想存储测试数据，那么设置```TEST_DATA_PERSISTENCE=None```即可)

目前支持的存储数据库为IoTDB和MySQL，以MySQL为例，你需要修改```config.properties```文件中的如下配置：

```properties
TEST_DATA_PERSISTENCE=MySQL
# 数据库的IP地址
TEST_DATA_STORE_IP=127.0.0.1
# 数据库的端口号
TEST_DATA_STORE_PORT=6667
# 数据库的名称
TEST_DATA_STORE_DB=result
# 数据库用户名
TEST_DATA_STORE_USER=root
# 数据库用户密码
TEST_DATA_STORE_PW=root
# 数据库读超时，单位毫秒
TEST_DATA_WRITE_TIME_OUT=300000
# 数据库写入并发池最多限制
TEST_DATA_MAX_CONNECTION=1
# 对本次实验的备注，作为表名的一部分存入数据库(如MySQL)中，注意不要有.等特殊字符
REMARK=
```

后续操作和上文保持一致。

## 6.7. 生成数据模式

### 6.7.1. Benchmark的配置

为了生成可以重复使用的数据集，iot-benchmark提供生成数据集的模式，生成数据集到FILE_PATH，以供后续使用正确性写入模式和正确性查询模式使用。

用户可以通过修改`BIG_BATCH_SIZE`来控制每个文件中包含的batch的个数

为此，你需要修改```config.properties```中的如下配置：

```
BENCHMARK_WORK_MODE=generateDataMode
# 数据集存储地址
FILE_PATH=data/test
DEVICE_NUMBER=5
SENSOR_NUMBER=10
CLIENT_NUMBER=5
BATCH_SIZE_PER_WRITE=10
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0
# 每个数据文件包含的Batch个数
BIG_BATCH_SIZE=100
```

> 注意：
> FILE_PATH文件夹应当为空文件夹，如果非空则会报错，生成的数据集会存放到这个文件夹中。

### 6.7.2. Benchmark的启动

您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.7.3. Benchmark的执行

生成数据启动后，你可以看到滚动的执行信息。当测试结束后，最后会显示出本次生成的数据集的信息，如下所示：

```
2022-05-08 15:07:19,641 INFO  cn.edu.tsinghua.iot.benchmark.mode.GenerateDataMode:43 - Data Location: data/test 
2022-05-08 15:07:19,641 INFO  cn.edu.tsinghua.iot.benchmark.mode.GenerateDataMode:44 - Schema Location: data/test/schema.txt 
2022-05-08 15:07:19,641 INFO  cn.edu.tsinghua.iot.benchmark.mode.GenerateDataMode:45 - Generate Info Location: data/test/info.txt 
```

> 注意：
> 1. 数据存放位置为FILE_PATH文件夹下，其目录结构为/d_xxx/batch_xxx.txt
> 2. 设备和传感器的相关元数据存放在FILE_PATH/schema.txt中
> 3. 数据集的相关信息存放在FILE_PATH/info.txt中

以下是info.txt的一个实例：

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

## 6.8. 正确性写入模式（单数据库，外部数据集）

为了验证数据集写入的正确性，您可以使用该模式写入生成数据模式中生成的数据集，目前该模式仅支持IoTDB v1.0 及更新的版本和InfluxDB v1.x

### 6.8.1. Benchmark的配置

为此，你需要修改```config.properties```中的如下配置：

```
BENCHMARK_WORK_MODE=verificationWriteMode
# 数据集存储地址
FILE_PATH=data/test
# 每个数据文件包含的Batch个数
BIG_BATCH_SIZE=100
```

> 注意：
> 1. FILE_PATH文件夹应当为使用生成数据模式生成的数据集
> 2. 运行该模式时其他参数应当和info.txt中的描述**保持一致**


外部数据集，即如果需要使用现有的真实数据进行扩展插入到数据库中，需要设置如下
```
BENCHMARK_WORK_MODE=verificationWriteMode
FILE_PATH=data/test
BIG_BATCH_SIZE=100
CLIENT_NUMBER=1
BATCH_SIZE_PER_WRITE=100
IS_COPY_MODE=true
```
需要在FILE_PATH中添加外部数据集
```
+ FILE_PATH
   + d_0
       + *.csv  # 将第一列修改为如"Sensor,s_0,s_1,..."
   + schema.txt # 每一行解释每个Sensor的Type,如"d_0 s_0 3\n d_0 s_1 4"
```
添加完成后，即可运行。

### 6.8.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.8.3. Benchmark的执行

写入数据启动后，你可以看到滚动的执行信息，其中部分信息如下：

```
...
2022-05-08 15:08:31,735 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-4 9.86% workload is done. 
2022-05-08 15:08:36,735 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-4 98.24% workload is done. 
2022-05-08 15:08:36,735 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-5 97.08% workload is done. 
2022-05-08 15:08:36,735 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-3 96.54% workload is done. 
...
```

当测试结束后，最后会显示出写入数据集的信息，如下所示：

```
2022-05-08 15:08:38,751 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=verificationWriteMode
########### Database Connection Information ###########
DOUBLE_WRITE=false
DBConfig=
  DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

## 6.9. 正确性单点查询模式（单数据库，外部数据集）

在运行这个模式之前需要先使用正确性写入模式写入数据到数据库。

为了验证数据集写入的正确性，您可以使用该模式查询写入到数据库中的数据集，目前该模式仅支持IoTDB v1.0 和 InfluxDB v1.x

### 6.9.1. Benchmark的配置

为此，你需要修改```config.properties```中的如下配置：

```
BENCHMARK_WORK_MODE=verificationQueryMode
# 数据集存储地址
FILE_PATH=data/test
# 每个数据文件包含的Batch个数
BIG_BATCH_SIZE=100
```

> 注意：
> 1. FILE_PATH文件夹应当为使用生成数据模式生成的数据集
> 2. 运行该模式时其他参数应当和info.txt中的描述**保持一致**

### 6.9.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.9.3. Benchmark的执行
写入数据启动后，你可以看到滚动的执行信息，其中部分信息如下：

```
...
2022-05-08 15:09:38,358 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-3 11.15% workload is done. 
2022-05-08 15:09:38,358 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-4 11.16% workload is done. 
2022-05-08 15:09:38,358 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-1 11.32% workload is done. 
2022-05-08 15:09:43,358 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-4 14.92% workload is done. 
...
```

当测试结束后，最后会显示出写入数据集的信息，如下所示：

```
2022-05-08 15:11:50,033 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=verificationQueryMode
########### Database Connection Information ###########
DOUBLE_WRITE=false
DBConfig=
  DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

## 6.10. 双数据库模式

为了更方便、快速完成正确性验证，iot-benchmark也支持双数据库模式。

1. 对于上文中提到的所有测试场景，除特别说明，均支持双数据库进行。请在`verification`项目中**启动测试**。
2. 对于下文中的正确性验证的相关测试场景，均必须在双数据库模式下运行，并且目前仅支持IoTDB v1.0 及更新的版本和timescaledb。

为了完成双数据库配置，您需要对`config.properties`完成如下修改：

```
################ Benchmark：双写模式 ####################
# 双写模式仅支持不同数据库之间进行比较，不支持同一个数据库不同版本进行双写
IS_DOUBLE_WRITE=true
# 另一个写入的数据库，目前的格式为{name}{-version}{-insert mode}(注意-号)其全部参考值参见README文件
ANOTHER_DB_SWITCH=TimescaleDB
# 另一个写入的数据库的主机
ANOTHER_HOST=127.0.0.1
# 另一个写入的数据库的端口
ANOTHER_PORT=5432
# 另一个写入的数据库的用户名
ANOTHER_USERNAME=postgres
# 另一个写入的数据库的密码，如果为多个数据库，则要求保持一致
ANOTHER_PASSWORD=postgres
# 另一个写入的数据库的名称
ANOTHER_DB_NAME=postgres
# 另一个数据库认证使用的Token，目前仅限于InfluxDB 2.0使用
ANOTHER_TOKEN=token
# 是否将两个数据库中的查询结果集进行比较
IS_COMPARISON=false
# 是否进行两个数据库间点对点数据对比
IS_POINT_COMPARISON=false
```

## 6.11. 常规测试模式之写入(双数据库)

为了进行下文中的正确性验证，首先需要将数据写到两个数据库中。

### 6.11.1. Benchmark的配置

如双数据库模式中描述的方式在`config.properties`完成双数据库配置

此外，请在`config.properties`中修改如下配置：

```
BENCHMARK_WORK_MODE=testWithDefaultPath
DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

### 6.11.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务，并且在5432端口启动TimescaleDB服务

之后您进入到`iot-benchmark/verfication/target/iot-benchmark-verification`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.11.3. Benchmark的执行

写入数据启动后，你可以看到滚动的执行信息，其中部分信息如下：

```
...
2022-05-12 09:47:51,233 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-10 91.40% workload is done.
2022-05-12 09:47:51,233 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-13 90.90% workload is done.
2022-05-12 09:47:51,233 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-16 92.50% workload is done.
2022-05-12 09:47:51,233 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-2 91.90% workload is done.
...
```

当测试结束后，最后会显示出写入数据集的信息，如下所示：

```
2022-05-12 09:48:00,160 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished.
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=true
DBConfig=
  DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

## 6.12. 正确性单点查询模式（双数据库比较）

为了更高效的验证数据库数据的正确性，iot-benchmark提供通过对比两个数据库间的数据来完成正确性验证。

注意，在进行该测试前，请先使用上文中的常规测试模式之写入（双数据库）完成数据库写入，目前建议使用JDBC方式。

### 6.12.1. Benchmark的配置

如双数据库模式中描述的方式在`config.properties`完成双数据库配置，其中修改如下配置，开始正确性单点查询（双数据库比较）

```
# 是否进行两个数据库间点对点数据对比
IS_POINT_COMPARISON=true
```

此外，请在`config.properties`中修改如下配置：

```
BENCHMARK_WORK_MODE=testWithDefaultPath
DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

### 6.12.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务，并且在5432端口启动TimescaleDB服务

之后您进入到`iot-benchmark/verfication/target/iot-benchmark-verification`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.12.3. Benchmark的执行

写入数据启动后，你可以看到滚动的执行信息，其中部分信息如下：

```
...
2022-05-12 09:49:51,591 INFO  cn.edu.tsinghua.iot.benchmark.client.generate.GenerateDataDeviceClient:83 - All points of d_11 have been checked 
2022-05-12 09:49:51,596 INFO  cn.edu.tsinghua.iot.benchmark.client.generate.GenerateDataDeviceClient:83 - All points of d_7 have been checked 
2022-05-12 09:49:51,596 INFO  cn.edu.tsinghua.iot.benchmark.client.generate.GenerateDataDeviceClient:83 - All points of d_16 have been checked 
2022-05-12 09:49:51,596 INFO  cn.edu.tsinghua.iot.benchmark.client.generate.GenerateDataDeviceClient:83 - All points of d_12 have been checked 
...
```

当测试结束后，最后会显示相关的信息，如下所示：

```
2022-05-12 09:49:53,669 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=true
DBConfig=
  DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

## 6.13. 正确性功能查询模式（双数据库比较）

为了更高效的验证数据库查询的正确性，iot-benchmark提供通过对比两个数据库间的数据查询结果的差异来完成正确性验证。

注意:

1. 在进行该测试前，请先使用上文中的常规测试模式之写入（双数据库）完成数据库写入。
2. LOOP的值**不能过大**，满足：LOOP(query) * QUERY_INTERVAL(query) * DEVICE_NUMBER(write) <= LOOP(write) * POINT_STEP(write)

### 6.13.1. Benchmark的配置

如双数据库模式中描述的方式在`config.properties`完成双数据库配置，其中修改如下配置，开始正确性单点查询（双数据库比较）

```
# 是否将两个数据库中的查询结果集进行比较
IS_COMPARISON=true
```

此外，请在`config.properties`中修改如下配置（注意：`LOOP=100`，避免查询超出写入范围）

```
BENCHMARK_WORK_MODE=testWithDefaultPath
DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

### 6.13.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务，并且在5432端口启动TimescaleDB服务

之后您进入到`iot-benchmark/verfication/target/iot-benchmark-verification`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.13.3. Benchmark的执行
写入数据启动后，你可以看到滚动的执行信息，其中部分信息如下：

```
...
2022-05-12 09:53:50,435 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-10 9.80% workload is done. 
2022-05-12 09:53:50,435 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-11 8.80% workload is done. 
2022-05-12 09:53:50,435 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-9 8.20% workload is done. 
2022-05-12 09:53:50,435 INFO  cn.edu.tsinghua.iot.benchmark.client.DataClient:137 - pool-2-thread-20 8.70% workload is done. 
...
```

当测试结束后，最后会显示出写入数据集的信息，如下所示：

```
2022-05-12 09:53:55,078 INFO  cn.edu.tsinghua.iot.benchmark.mode.BaseMode:154 - All dataClients finished. 
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=true
DBConfig=
  DB_SWITCH=IoTDB-100-SESSION_BY_TABLET
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

# 7. 使用IoTDB Benchmark测试其他数据库(部分)

## 7.1. 测试 InfluxDB v1.x
[快速指引](influxdb/README.md)

## 7.2. 测试 InfluxDB v2.0
[快速指引](influxdb-2.0/README.md)

## 7.3. 测试 Microsoft SQL Server
[快速指引](mssqlserver/README.md)

## 7.4. 测试 QuestDB
[快速指引](questdb/README.md)

## 7.5. 测试 SQLite
[快速指引](sqlite/README.md)

## 7.6. 测试 Victoriametrics
[快速指引](victoriametrics/README.md)

## 7.7. 测试 TimeScaleDB
[快速指引](timescaledb/README.md)

## 7.8. 测试 PI Archive

[快速指引](./pi/README.md)

## 7.9. 测试 TDengine
[快速指引](./tdengine/README.md)

# 8. 正确性验证的进一步说明
1. 目前正确性验证部分仅支持IoTDB v1.0 及更新的版本和TimeScaleDB
2. [快速指引](verification/README.md)

# 9. 自动化脚本

## 9.1. 一键化启动脚本
您可以通过`cli-benchmark.sh`脚本一键化启动IoTDB、监控的IoTDB Benchmark和测试的IoTDB Benchmark，但需要注意该脚本启动时会清理IoTDB中的**所有数据**，请谨慎使用。

首先，您需要修改`cli-benchmark.sh`中的`IOTDB_HOME`参数为您本地的IoTDB所在的文件夹。

然后您可以使用脚本启动测试

```sh
> ./cli-benchmark.sh
```

测试完成后您可以在`logs`文件夹中查看测试相关日志，在`server-logs`文件夹中查看监控相关日志。

## 9.2. 自动执行多项测试

通常，除非与其他测试结果进行比较，否则单个测试是没有意义的。因此，我们提供了一个接口来通过一次启动执行多个测试。

### 9.2.1. 配置 routine

这个文件的每一行应该是每个测试过程会改变的参数（否则就变成复制测试）。例如，"例程"文件是：

```
LOOP=10 DEVICE_NUMBER=100 TEST
LOOP=20 DEVICE_NUMBER=50 TEST
LOOP=50 DEVICE_NUMBER=20 TEST
```

然后依次执行3个LOOP参数分别为10、20、50的测试过程。

> 注意：
> 您可以使用“LOOP=20 DEVICE_NUMBER=10 TEST”等格式更改每个测试中的多个参数，不允许使用不必要的空间。 关键字"TEST"意味着新的测试开始。如果您更改不同的参数，更改后的参数将保留在下一次测试中。

### 9.2.2. 开始测试

配置文件routine后，您可以通过启动脚本启动多测试任务：

```sh
> ./rep-benchmark.sh
```

然后测试信息将显示在终端中。

> 注意：
> 如果您关闭终端或失去与客户端机器的连接，测试过程将终止。 如果输出传输到终端，则与任何其他情况相同。

使用此接口通常需要很长时间，您可能希望将测试过程作为守护程序执行。这样，您可以通过启动脚本将测试任务作为守护程序启动：

```sh
> ./rep-benchmark.sh > /dev/null 2>&1 &
```

在这种情况下，如果您想知道发生了什么，可以通过以下命令查看日志信息：

```sh
> cd ./logs
> tail -f log_info.log
```

# 10. 相关文章
Benchmark Time Series Database with iot-benchmark for IoT Scenarios

Arxiv: https://arxiv.org/abs/1901.08304
