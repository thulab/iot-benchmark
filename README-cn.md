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
|        IoTDB         |   v1.x   |                           jdbc
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

---

# 5. 安装

## 5.1 环境准备：

为了使用iot-benchmark，你需要拥有：

1. Java 8
2. Maven：不建议使用镜像源，国内可以使用阿里云镜像源。
3. 合适版本的数据库
   1. Apache IoTDB >= v1.0([获取方式](https://github.com/apache/iotdb))
   2. 其他的对应版本的数据库

提示：--CSV的记录模式只能在Linux系统中使用，记录测试过程中的相关系统信息。
     --我们建议使用MacOs或Linux系统，本文以MacOS和Linux系统为例，如果使用Windows系统，请使用`conf`文件夹下的`benchmark.bat`脚本启动benchmark。
     
## 5.2 安装过程

在确保以上条件均以满足后，从 git 克隆源代码:

```
git clone https://github.com/apache/iotdb.git
```

默认的主分支是master分支，如果你想使用其他分支，请在克隆后进入项目根目录，并使用以下命令查看所有可用分支:

```
git branch -a
```

找到你希望工作的分支名后，使用以下命令切换到该分支：

```
git checkout [分支名]
```

使用如下命令通过Maven完成iot-benchmark的构建：

```
mvn clean package -Dmaven.test.skip=true
```
该命令会编译iot-benchmark的core模块，和所有其他相关的数据库。

# 6. iot-benchmark的使用

## 6.1 快速开始

提示：本文档中所有测试结果均出自如下环境：

```
CPU：I7-11700
内存：32G DDR4
系统盘：512G SSD (INTEL SSDPEKNU512GZ)
数据盘：2T HDD (WDC WD40EZAZ-00SF3B0)
```


在完成编译后，以IoTDB v1.0为例，**您需要首先在本机的6667端口启动相应版本的IoTDB服务**。（如果您对于使用IoTDB仍有疑问，请参照 https://github.com/apache/iotdb/blob/master/README_ZH.md 中的指引）。在成功启动IoTDB服务后，您可以进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0/iot-benchmark-iotdb-1.0`文件夹下，使用`./benchmark.sh`来启动对IoTDB v1.0的测试。我们推荐使用匹配的版本进行测试，以此达到最佳效果。


```

cd iotdb-1.0/target/iot-benchmark-iotdb-1.0/iot-benchmark-iotdb-1.0
./benchmark.sh
```

这将使用默认配置进行一次写入测试,测试启动后，如果执行无误，您将看到滚动的测试执行信息，其中部分信息如下：


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




配置文件存放在`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0/iot-benchmark-iotdb-1.0conf`下。当然，对于其他支持数据库您同样可以在相似的路径下找到其配置文件。编辑该文件来自定义测试的类型以及相关配置，**请注意，每次测试前您必须将配置文件中的DB_SWITCH参数更改为与待测数据库相匹配，其对应关系和可能取值如下所示：**


|        数据库        |   版本   |     对应子项目      |                                                  DB_SWITCH                                                   |
| :------------------: | :------: | :-----------------: | :----------------------------------------------------------------------------------------------------------: |
|   IoTDB(1.0/1.1/1.3) |   1.x    |      iotdb-1.x      | IoTDB-1x0-JDBC<br>IoTDB-1x0-SESSION_BY_TABLET<br>IoTDB-1x0-SESSION_BY_RECORD<br>IoTDB-1x0-SESSION_BY_RECORDS |
|       InfluxDB       |   1.x    |      influxdb       |                                                   InfluxDB                                                   |
|       InfluxDB       |   2.0    |    influxdb-2.0     |                                                 InfluxDB-2.0                                                 |
|       QuestDB        |  6.0.7   |       questdb       |                                                   QuestDB                                                    |
| Microsoft SQL Server | 2016 SP2 |     mssqlserver     |                                                 MSSQLSERVER                                                  |
|   VictoriaMetrics    |  1.64.0  |   victoriametrics   |                                               VictoriaMetrics                                                |
|     TimescaleDB      |    --    |     timescaledb     |                                                 TimescaleDB                                                  |
|     TimescaleDB      | Cluster  | timescaledb-cluster |                                             TimescaleDB-Cluster                                              |
|        SQLite        |    --    |       sqlite        |                                                    SQLite                                                    |
|       OpenTSDB       |    --    |      opentsdb       |                                                   OpenTSDB                                                   |
|       KairosDB       |    --    |      kairosdb       |                                                   KairosDB                                                   |
|       TDengine       | 2.2.0.2  |      TDengine       |                                                   TDengine                                                   |
|       TDengine       |  3.0.1   |      TDengine       |                                                  TDengine-3                                                  |
|      PI Archive      |   2016   |      PIArchive      |                                                  PIArchive                                                   |


同时，您还可以更改BENCHMARK_WORK_MODE参数调整iot-benchmark的运行模式，目前已经支持：


|    模式名称    |  BENCHMARK_WORK_MODE  | 模式内容                                                                         |
| :------------: | :-------------------: | :------------------------------------------------------------------------------- |
|  常规测试模式  |  testWithDefaultPath  | 支持多种读和写操作的混合负载                                                     |
|  生成数据模式  |   generateDataMode    | Benchmark生成数据集到FILE_PATH路径中                                             |
| 正确性写入模式 | verificationWriteMode | 从FILE_PATH路径中加载数据集进行写入，目前支持 IoTDB v1.0 及更新的版本           |
| 正确性查询模式 | verificationQueryMode | 从FILE_PATH路径中加载数据集和数据库中进行比对，目前支持 IoTDB v1.0 及更新的版本 |


其他可变参数配置以及注解请见[config.properties](configuration/conf/config.properties)，此处不做过多展开。


## 6.2 iot-benchmark的不同运行模式

再次提醒，在你开始任何一个新的测试用例前，你都需要确认配置文件```config.properties```中的配置信息是否符合预期。

### 6.2.1 常规测试模式之写入(单数据库)


假设工作负载参数是：


```
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
| Measurement/StorageGroup | tag/device | field/sensor | concurrency | batch size | point interval(ms) |  loop  |
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
|            10            |    50      |      500     |     20      |    100     |        200         |  10000 |
+--------------------------+------------+--------------+-------------+------------+--------------------+--------+
```

注意：此配置下总的时间序列数为：```deivce * sensor = 25,000```，每个时间序列的点数为 ```batch size * loop = 20,000```，
总数据点数为 ```deivce * sensor * batch size * loop = 500,000,000```。 每个数据点占用空间大小可以按 16 字节估计，则原始数据总大小为 8G。

那么对应的，需要修改```config.properties```文件如下所示。请在完成修改后去掉相应配置项前的```#```以确保改动生效：

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

此处已经介绍了一些参数的基本修改操作，之后的部分将省略相关设置，如有需要请进入如下页面查看。
[Testconfigurations](https://github.com/supersshhhh/iot-benchmark/blob/patch-1/TestConfigurations.md)

## 6.2.2 常规测试模式之查询(单数据库，不使用系统记录)

常规测试模式除了用于写入数据，还可以仅仅查询数据。


## 6.2.3 常规测试模式之读写混合模式（单数据库）

常规测试模式可以支持用户进行读写混合的测试，需要注意的是这种场景下的读写混合的时间戳都是从**写入开始时间**开始。


## 6.2.4 常规测试模式之读写混合模式（单数据库，查询最近写入数据）

常规测试模式可以支持用户进行读写混合的测试(查询最近写入数据），需要注意的是这种场景下的查询时间范围为当前最大写入时间戳的左侧临近的数据。


## 6.2.5 常规测试模式之使用系统记录（单数据库）

IoTDB Benchmark支持您使用数据库存储测试过程中的系统数据，目前支持使用CSV记录。


## 6.2.6 常规测试模式之测试过程持久化（单数据库）

为了后续的分析，iot-benchmark可以将测试信息存储到数据库中(如果你不想存储测试数据，那么设置```TEST_DATA_PERSISTENCE=None```即可)


## 6.2.7 生成数据模式

为了生成可以重复使用的数据集，iot-benchmark提供生成数据集的模式，生成数据集到FILE_PATH，以供后续使用正确性写入模式和正确性查询模式使用。


## 6.2.8 正确性写入模式（单数据库，外部数据集）

为了验证数据集写入的正确性，您可以使用该模式写入生成数据模式中生成的数据集，目前该模式仅支持IoTDB v1.0 及更新的版本和InfluxDB v1.x


## 6.2.9 正确性单点查询模式（单数据库，外部数据集）

在运行这个模式之前需要先使用正确性写入模式写入数据到数据库。为了验证数据集写入的正确性，您可以使用该模式查询写入到数据库中的数据集，目前该模式仅支持IoTDB v1.0 和 InfluxDB v1.x。


## 6.2.10 双数据库模式

为了更方便、快速完成正确性验证，iot-benchmark也支持双数据库模式。

1. 对于上文中提到的所有测试场景，除特别说明，均支持双数据库进行。请在`verification`项目中**启动测试**。
2. 对于下文中的正确性验证的相关测试场景，均必须在双数据库模式下运行，并且目前仅支持IoTDB v1.0 及更新的版本和timescaledb。


## 6.2.11 常规测试模式之写入(双数据库)

为了进行下文中的正确性验证，首先需要将数据写到两个数据库中。


## 6.2.12 正确性单点查询模式（双数据库比较）

为了更高效的验证数据库数据的正确性，iot-benchmark提供通过对比两个数据库间的数据来完成正确性验证。注意，在进行该测试前，请先使用上文中的常规测试模式之写入（双数据库）完成数据库写入，目前建议使用JDBC方式。


## 6.2.13 正确性功能查询模式（双数据库比较）

为了更高效的验证数据库查询的正确性，iot-benchmark提供通过对比两个数据库间的数据查询结果的差异来完成正确性验证。

注意:

1. 在进行该测试前，请先使用上文中的常规测试模式之写入（双数据库）完成数据库写入。
2. LOOP的值**不能过大**，满足：LOOP(query) * QUERY_INTERVAL(query) * DEVICE_NUMBER(write) <= LOOP(write) * POINT_STEP(write)


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
