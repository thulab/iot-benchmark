# 1. IoTDB-Benchmark
![](https://img.shields.io/badge/platform-MacOS%20%7C%20Linux%20%7C%20Windows-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)

# 2. 内容目录

<!-- TOC -->

- [1. IoTDB-Benchmark](#1-iotdb-benchmark)
- [2. 内容目录](#2-内容目录)
- [3. 概述](#3-概述)
- [4. 主要特点](#4-主要特点)
- [5. IoTDB-Benchmark的使用](#5-iotdb-benchmark的使用)
  - [5.1. IoTDB-Benchmark运行的前置条件](#51-iotdb-benchmark运行的前置条件)
  - [5.2. IoTDB-Benchmark支持的运行模式](#52-iotdb-benchmark支持的运行模式)
  - [5.3. IoTDB-Benchmark的编译构建](#53-iotdb-benchmark的编译构建)
- [6. IoTDB-Benchmark的不同运行模式的说明](#6-iotdb-benchmark的不同运行模式的说明)
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
  - [7.9. 测试 TDenginee](#79-测试-tdenginee)
- [8. 正确性验证的进一步说明](#8-正确性验证的进一步说明)
- [9. 自动执行多项测试](#9-自动执行多项测试)
  - [9.1. 配置 routine](#91-配置-routine)
  - [9.2. 开始测试](#92-开始测试)
- [10. 开发者指引](#10-开发者指引)
- [11. 相关文章](#11-相关文章)

<!-- /TOC -->

# 3. 概述

IoTDB-Benchmark是用来将IoTDB和其他数据库和时间序列解决方案进行基准测试的工具。

目前支持如下数据库、版本和连接方式：

|        数据库        |   版本   |                         连接方式                         |
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
|       TDengine       | 2.2.0.2  |                           jdbc                           |
|      PI Archive      |   2016   |                           jdbc                           |


# 4. 主要特点

IotDB-Benchmark的特点如下：

1. 使用方便：IoTDB-benchmark是一个结合了多种测试功能的工具，用户不需要切换不同的工具。
2. 多种数据插入和测试模式：
   1. 按照配置生成周期性的时间序列数据并直接插入和查询。
   2. 将生成的数据写入到磁盘中对应位置。
   3. 从磁盘中生成的生成的数据集加载数据，并写入和查询。
   4. 对数据和查询结果分别进行正确性验证测试。
3. 测试报告与结果：支持存储测试信息和结果以供进一步查询或分析。
4. 可视化测试结果：与Tableau集成以可视化测试结果。

# 5. IoTDB-Benchmark的使用

## 5.1. IoTDB-Benchmark运行的前置条件

为了使用IoTDB-Benchmark，你需要拥有：

1. Java 8
2. Maven：不建议使用镜像源，国内可以使用阿里云镜像源。
3. 合适版本的数据库
   1. Apache IoTDB >= v0.9([获取方式](https://github.com/apache/iotdb))，并且目前主要支持IoTDB v0.12
   2. 其他的对应版本的数据库
4. ServerMode和CSV的记录模式只能在Linux系统中使用，记录测试过程中的相关系统信息。
5. 我们建议使用MacOs或Linux系统，本文以MacOS和Linux系统为例，如果使用Windows系统，请使用`conf`文件夹下的`benchmark.bat`脚本启动benchmark。

## 5.2. IoTDB-Benchmark支持的运行模式
|        模式名称        |  BENCHMARK_WORK_MODE  | 模式内容                                                                               |
| :--------------------: | :-------------------: | :------------------------------------------------------------------------------------- |
|      常规测试模式      |  testWithDefaultPath  | 支持多种读和写操作的混合负载                                                           |
|      生成数据模式      |   generateDataMode    | Benchmark生成数据集到FILE_PATH路径中                                                   |
|     正确性写入模式     | verificationWriteMode | 从FILE_PATH路径中加载数据集进行写入，目前支持IoTDB v0.12                               |
|     正确性查询模式     | verificationQueryMode | 从FILE_PATH路径中加载数据集和数据库中进行比对，目前支持IoTDB v0.12                     |
| 服务器资源使用监控模式 |      serverMODE       | 服务器资源使用监控模式（该模式下运行通过ser-benchmark.sh脚本启动，无需手动配置该参数） |


## 5.3. IoTDB-Benchmark的编译构建

你可以使用Maven完成IoTDB-Benchmark的构建，在项目**根目录**中使用如下命令：

```sh
mvn clean package -Dmaven.test.skip=true
```

该命令会编译IoTDB-Benchmark的core模块，和所有其他相关的数据库。

在完成编译后，以IoTDB v0.12为例，你可以进入到`iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`文件夹下，使用`./benchmark.sh`来启动对IoTDB v0.12的测试。

默认的配置文件存放在`iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1/conf`下，您可以编辑`config.properties`来完成配置，请**注意的是，您需要将配置文件中的DB_SWITCH参数调整为您需要被测数据库**，其对应关系和可能取值如下所示：

|        数据库        |   版本   |   对应子项目    |                                                  DB_SWITCH                                                   |
| :------------------: | :------: | :-------------: | :----------------------------------------------------------------------------------------------------------: |
|        IoTDB         |   0.12   |   iotdb-0.12    | IoTDB-012-JDBC<br>IoTDB-012-SESSION_BY_TABLET<br>IoTDB-012-SESSION_BY_RECORD<br>IoTDB-012-SESSION_BY_RECORDS |
|        IoTDB         |   0.11   |   iotdb-0.11    |                        IoTDB-011-JDBC<br>IoTDB-011-SESSION<br>IoTDB-011-SESSION_POOL                         |
|        IoTDB         |   0.10   |   iotdb-0.10    |                                     IoTDB-010-JDBC<br>IoTDB-010-SESSION                                      |
|        IoTDB         |   0.9    |   iotdb-0.09    |                                      IoTDB-09-JDBC<br>IoTDB-09-SESSION                                       |
|       InfluxDB       |   v1.x   |    influxdb     |                                                   InfluxDB                                                   |
|       InfluxDB       |   v2.0   |  influxdb-2.0   |                                                 InfluxDB-2.0                                                 |
|       QuestDB        |  v6.0.7  |     questdb     |                                                   QuestDB                                                    |
| Microsoft SQL Server | 2016 SP2 |   mssqlserver   |                                                 MSSQLSERVER                                                  |
|   VictoriaMetrics    | v1.64.0  | victoriametrics |                                               VictoriaMetrics                                                |
|     TimescaleDB      |          |   timescaledb   |                                                 TimescaleDB                                                  |
|        SQLite        |    --    |     sqlite      |                                                    SQLite                                                    |
|       OpenTSDB       |    --    |    opentsdb     |                                                   OpenTSDB                                                   |
|       KariosDB       |    --    |    kairosdb     |                                                   KairosDB                                                   |
|       TDengine       | 2.2.0.2  |    TDengine     |                                                   TDengine                                                   |
|      PI Archive      |   2016   |    PIArchive    |                                                  PIArchive                                                   |

# 6. IoTDB-Benchmark的不同运行模式的说明

## 6.1. 常规测试模式之写入(单数据库)

这个简单的指引将以常规测试模式为例带你快速熟悉IoTDB-Benchmark的使用基本流程。

### 6.1.1. Benchmark的配置

在你开始任何一个新的测试用例前，你都需要确认配置文件```config.properties```中的配置信息。为了您的方便，我们已经为以下演示设置了默认配置。

假设您要测试 IoTDB 的数据库性能。您已经安装了IoTDB依赖项并使用默认设置启动了IoTDB服务器。服务器的IP是127.0.0.1。假设工作负载参数是：

```
+--------------------------+------------+--------------+-------------+------------+-------------------+------+
| Measurement/StorageGroup | tag/device | field/sensor | concurrency | batch size | point interval(s) | loop |
+--------------------------+------------+--------------+-------------+------------+-------------------+------+
|            20            |    20      |      300     |     20      |      1     |         5         | 1000 |
+--------------------------+------------+--------------+-------------+------------+-------------------+------+
```

那么对应的，需要修改```config.properties```文件如下所示：

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

当然，你也可以其他参数，更多的配置参见[config.properties](configuration/conf/config.properties)。

### 6.1.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.1.3. Benchmark的执行

测试启动后，你可以看到滚动的测试执行信息，其中部分信息如下：

```
...
19:39:51.392 [pool-16-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-5 20.30% workload is done.
19:39:51.392 [pool-57-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-19 19.50% workload is done.
19:39:51.392 [pool-40-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-13 20.50% workload is done.
19:39:51.392 [pool-43-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-14 23.80% workload is done.
...
```

当测试结束后，最后会显示出本次测试的统计信息，如下所示：

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

> 注意：
> `config.properties`中包含了查询相关的配置参数，您可以通过查看示例文件来了解。

### 6.2.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.2.3. Benchmark的执行

测试启动后，你可以看到滚动的测试执行信息，其中部分信息如下：

```
...
19:42:42.418 [pool-61-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-20 60.70% workload is done.
19:42:42.418 [pool-37-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-12 59.90% workload is done.
19:42:42.418 [pool-16-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-5 60.10% workload is done.
19:42:42.418 [pool-34-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-11 60.10% workload is done.
...
```

当测试结束后，最后会显示出本次测试的统计信息，如下所示：

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

### 6.3.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.3.3. Benchmark的执行

测试启动后，你可以看到滚动的测试执行信息，其中部分信息如下：

```
...
19:54:38.983 [pool-16-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-5 84.30% workload is done.
19:54:38.983 [pool-55-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-18 86.00% workload is done.
19:54:38.983 [pool-40-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-13 84.00% workload is done.
19:54:38.983 [pool-46-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-15 85.10% workload is done.
...
```

当测试结束后，最后会显示出本次测试的统计信息，如下所示：

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


## 6.4. 常规测试模式之读写混合模式（单数据库，查询最近写入数据）
常规测试模式可以支持用户进行读写混合的测试(查询最近写入数据），需要注意的是这种场景下的查询时间范围为当前最大写入时间戳的左侧临近的数据。

### 6.4.1. Benchmark的配置

修改```config.properties```文件中的相关参数如下(其中格外注意设置```IS_RECENT_QUERY=true```，来关闭最近查询模式)：

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

### 6.4.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.4.3. Benchmark的执行

测试启动后，你可以看到滚动的测试执行信息，其中部分信息如下：

```
...
19:56:24.503 [pool-52-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-17 73.90% workload is done.
19:56:24.503 [pool-34-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-11 73.60% workload is done.
19:56:24.503 [pool-40-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-13 75.20% workload is done.
19:56:24.503 [pool-46-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-15 80.50% workload is done.
...
```

当测试结束后，最后会显示出本次测试的统计信息，如下所示：

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

## 6.5. 常规测试模式之使用系统记录（单数据库）

IoTDB Benchmark支持您使用数据库存储测试过程中的系统数据，目前支持使用CSV记录。

### 6.5.1. Benchmark的配置

假设您的 IoTDB 服务器 IP 是 192.168.130.9，并且您安装了 IoTDB-benchmark 的测试客户端服务器已授权访问 IoTDB 服务器。

当前版本的信息记录依赖于 iostat。请确保 iostat 已安装在 IoTDB 服务器中。

之后配置```config.properties```
假设您使用的参数与[简单指引](#61-常规测试模式之写入简单示例)中的参数相同。您应该添加的新参数是 TEST_DATA_PERSISTENCE 和 MONITOR_INTERVAL，例如：

```properties
TEST_DATA_PERSISTENCE=CSV
MONITOR_INTERVAL=0
```

> 1. TEST_DATA_PERSISTENCE=CSV 表示测试结果保存到CSV中。
> 2. INTERVAL=0 表示服务器信息记录的间隔最小为 2 秒。 如果您设置 INTERVAL=n，那么间隔将为 n+2 秒，因为记录过程至少需要2秒。在进行长时间测试时，您可能希望将 INTERVAL 设置得更长。

### 6.5.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

其他后续过程和上文所述类似，最后生成的相关文件会存放在```data```目录下。

## 6.6. 常规测试模式之测试过程持久化（单数据库）

为了后续的分析，IoTDB-Benchmark可以将测试信息存储到数据库中(如果你不想存储测试数据，那么设置```TEST_DATA_PERSISTENCE=None```即可)

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

为了生成可以重复使用的数据集，IoTDB-Benchmark提供生成数据集的模式，生成数据集到FILE_PATH，以供后续使用正确性写入模式和正确性查询模式使用。

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

您进入到`iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.7.3. Benchmark的执行

生成数据启动后，你可以看到滚动的执行信息，其中部分信息如下：

```
...
11:26:43.104 [pool-4-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-3 83.70% workload is done.
11:26:43.104 [pool-2-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-1 83.80% workload is done.
11:26:43.104 [pool-3-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-2 83.40% workload is done.
11:26:43.104 [pool-5-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-4 83.70% workload is done.
11:26:43.104 [pool-6-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-5 83.40% workload is done.
...
```

当测试结束后，最后会显示出本次生成的数据集的信息，如下所示：

```
11:26:43.286 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.GenerateDataMode - Data Location: data/test
11:26:43.287 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.GenerateDataMode - Schema Location: data/test/schema.txt
11:26:43.287 [main] INFO cn.edu.tsinghua.iotdb.benchmark.mode.GenerateDataMode - Generate Info Location: data/test/info.txt
```

> 注意：
> 1. 数据存放位置为FILE_PATH文件夹下，其目录结构为/d_xxx/batch_xxx.txt
> 2. 设备和传感器的相关元数据存放在FILE_PATH/schema.txt中
> 3. 数据集的相关信息存放在FILE_PATH/info.txt中

以下是info.txt的一个实例：

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

## 6.8. 正确性写入模式（单数据库，外部数据集）

为了验证数据集写入的正确性，您可以使用该模式写入生成数据模式中生成的数据集，目前该模式仅支持IoTDB v0.12

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

### 6.8.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.8.3. Benchmark的执行

写入数据启动后，你可以看到滚动的执行信息，其中部分信息如下：

```
...
11:30:02.615 [pool-9-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-3 55.60% workload is done.
11:30:02.615 [pool-6-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-2 55.40% workload is done.
11:30:02.615 [pool-12-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-4 54.20% workload is done.
11:30:02.615 [pool-4-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-1 55.00% workload is done.
11:30:02.615 [pool-15-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-5 55.00% workload is done.
...
```

当测试结束后，最后会显示出写入数据集的信息，如下所示：

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

## 6.9. 正确性单点查询模式（单数据库，外部数据集）

在运行这个模式之前需要先使用正确性写入模式写入数据到数据库。

为了验证数据集写入的正确性，您可以使用该模式查询写入到数据库中的数据集，目前该模式仅支持IoTDB v0.12

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

之后您进入到`iotdb-benchmark/iotdb-0.12/target/iotdb-0.12-0.0.1`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.9.3. Benchmark的执行
写入数据启动后，你可以看到滚动的执行信息，其中部分信息如下：

```
...
11:31:32.595 [pool-2-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-1 50.60% workload is done.
11:31:32.595 [pool-6-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-3 51.30% workload is done.
11:31:32.595 [pool-4-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-2 51.60% workload is done.
11:31:32.595 [pool-10-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-5 50.90% workload is done.
11:31:32.595 [pool-8-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-4 51.40% workload is done.
...
```

当测试结束后，最后会显示出写入数据集的信息，如下所示：

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

## 6.10. 双数据库模式

为了更方便、快速完成正确性验证，iotdb-benchmark也支持双数据库模式。

1. 对于上文中提到的所有测试场景，除特别说明，均支持双数据库进行。请在`verification`项目中**启动测试**。
2. 对于下文中的正确性验证的相关测试场景，均必须在双数据库模式下运行，并且目前仅仅支持iotdb-0.12和timescaledb两种。

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

### 6.11.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务，并且在5432端口启动TimescaleDB服务

之后您进入到`iotdb-benchmark/verfication/target/verification-0.0.1`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.11.3. Benchmark的执行

写入数据启动后，你可以看到滚动的执行信息，其中部分信息如下：

```
...
11:49:14.691 [pool-55-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-18 89.90% workload is done.
11:49:14.691 [pool-16-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-5 90.40% workload is done.
11:49:14.692 [pool-58-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-19 88.50% workload is done.
11:49:14.692 [pool-19-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-6 90.40% workload is done.
11:49:14.691 [pool-52-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-17 88.10% workload is done.
...
```

当测试结束后，最后会显示出写入数据集的信息，如下所示：

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

## 6.12. 正确性单点查询模式（双数据库比较）

为了更高效的验证数据库数据的正确性，iotdb-benchmark提供通过对比两个数据库间的数据来完成正确性验证。

注意，在进行该测试前，请先使用上文中的常规测试模式之写入（双数据库）完成数据库写入。

### 6.12.1. Benchmark的配置

如双数据库模式中描述的方式在`config.properties`完成双数据库配置，其中修改如下配置，开始正确性单点查询（双数据库比较）

```
# 是否进行两个数据库间点对点数据对比
IS_POINT_COMPARISON=true
```

此外，请在`config.properties`中修改如下配置：

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

### 6.12.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务，并且在5432端口启动TimescaleDB服务

之后您进入到`iotdb-benchmark/verfication/target/verification-0.0.1`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.12.3. Benchmark的执行

写入数据启动后，你可以看到滚动的执行信息，其中部分信息如下：

```
...
11:53:02.347 [pool-74-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper - pool-1-thread-11 97.90% syntheticClient for d_0 is done.
11:53:02.347 [pool-73-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper - pool-1-thread-12 98.90% syntheticClient for d_0 is done.
11:53:02.354 [pool-76-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper - pool-1-thread-4 100.00% syntheticClient for d_0 is done.
...
```

当测试结束后，最后会显示出写入数据集的信息，如下所示：

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

## 6.13. 正确性功能查询模式（双数据库比较）

为了更高效的验证数据库查询的正确性，iotdb-benchmark提供通过对比两个数据库间的数据查询结果的差异来完成正确性验证。

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

### 6.13.2. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务，并且在5432端口启动TimescaleDB服务

之后您进入到`iotdb-benchmark/verfication/target/verification-0.0.1`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

### 6.13.3. Benchmark的执行
写入数据启动后，你可以看到滚动的执行信息，其中部分信息如下：

```
...
12:08:21.087 [pool-37-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-12 70.00% workload is done.
12:08:21.087 [pool-55-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-18 68.00% workload is done.
12:08:21.087 [pool-43-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-14 78.00% workload is done.
12:08:21.087 [pool-46-thread-1] INFO cn.edu.tsinghua.iotdb.benchmark.client.Client - pool-1-thread-15 80.00% workload is done.
...
```

当测试结束后，最后会显示出写入数据集的信息，如下所示：

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

## 7.9. 测试 TDenginee
[快速指引](./tdengine/README.md)

# 8. 正确性验证的进一步说明
1. 目前正确性验证部分仅支持IoTDB v0.12和TimeScaleDB
2. [快速指引](verification/README.md)

# 9. 自动执行多项测试

通常，除非与其他测试结果进行比较，否则单个测试是没有意义的。因此，我们提供了一个接口来通过一次启动执行多个测试。

## 9.1. 配置 routine

这个文件的每一行应该是每个测试过程会改变的参数（否则就变成复制测试）。例如，"例程"文件是：

```
LOOP=10 DEVICE_NUMBER=100 TEST
LOOP=20 DEVICE_NUMBER=50 TEST
LOOP=50 DEVICE_NUMBER=20 TEST
```

然后依次执行3个LOOP参数分别为10、20、50的测试过程。

> 注意：
> 您可以使用“LOOP=20 DEVICE_NUMBER=10 TEST”等格式更改每个测试中的多个参数，不允许使用不必要的空间。 关键字"TEST"意味着新的测试开始。如果您更改不同的参数，更改后的参数将保留在下一次测试中。

## 9.2. 开始测试

配置文件routine后，还需要修改rep-benchmark.sh和dea-benchmark.sh。您需要将 cli-benchmark.sh 更改为 benchmark.sh

```sh
sh $BENCHMARK_HOME/benchmark.sh
```

在运行之前，您可以通过启动脚本启动多测试任务：

```sh
> ./rep-benchmark.sh
```

然后测试信息将显示在终端中。

> 注意：
> 如果您关闭终端或失去与客户端机器的连接，测试过程将终止。 如果输出传输到终端，则与任何其他情况相同。

使用此接口通常需要很长时间，您可能希望将测试过程作为守护程序执行。这样，您可以通过启动脚本将测试任务作为守护程序启动：

```sh
> ./dae-benchmark.sh
```

在这种情况下，如果您想知道发生了什么，可以通过以下命令查看日志信息：

```sh
> cd ./logs
> tail -f log_info.log
```

# 10. 开发者指引
1. IoTDB-Benchmark的所有的接口均在core模块中。
2. IoTDB-Benchmark的所有的数据库测试的实现均在各个maven子项目中。
3. 如果你想要使用IDEA等编辑器运行Benchmark：
   1. 可以在每一个maven子项目下找到test文件目录下的TestEntrance，运行对应测试。
   2. 以IoTDB 0.12为例，你可以运行`iotdb-0.12/src/main/test/cn/edu/tsinghua/iotdb/benchmark/TestEntrance`

# 11. 相关文章
Benchmark Time Series Database with IoTDB-Benchmark for IoT Scenarios

Arxiv: https://arxiv.org/abs/1901.08304