IoT Benchmark
---
![](https://img.shields.io/badge/platform-MacOS%20%7C%20Linux%20%7C%20Windows-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)

- [1. 概述](#1-概述)
- [2. 支持的数据库类型](#2-支持的数据库类型)
- [3. 快速开始](#3-快速开始)
  - [3.1. 前置环境](#31-前置环境)
  - [3.2. 获取方式](#32-获取方式)
  - [3.3. 快速开始](#33-快速开始)
- [4. 相关文章](#4-相关文章)

# 1. 概述

![IoT Benchmark 架构](https://github.com/apache/iotdb/assets/46039728/1fbf027d-0955-4de6-b727-57dbeaf2a5ab)

IoT-Benchmark 是用来评估时序数据库、实时数据库在工业物联网（IIoT）场景下的性能的基准测试工具，具有如下特点：
1. **跨平台支持**：支持主流操作系统，通过简单命令即可启动测试并获得结果。
2. **集多种测试功能于一身，能够满足多样化的测试需求**：
   1. 按照配置生成周期性的时间序列数据并直接插入和查询。
   2. 将生成的数据写入到磁盘中对应位置。
   3. 从磁盘中生成的生成的数据集加载数据，并写入和查询。
   4. 对数据和查询结果分别进行正确性验证测试。
3. **支持多种类型的数据库**：支持 InfluxDB、IoTDB 等市面上主流的时序数据库、实时数据库产品。
4. **多元化的测试报告生成**：支持以文件、MySQL 等多种形式存储测试基本信息与结果以供进一步查询或分析。
5. **测试结果可视化**：与 Tableau 集成以可视化测试结果。

# 2. 支持的数据库类型

目前支持如下数据库、版本和连接方式：

|        数据库        |    版本    |
| :------------------: | :--------: |
|        IoTDB         |    v1.x    |
|       InfluxDB       | v1.x、v2.x |
|       QuestDB        |   v6.0.7   |
| Microsoft SQL Server |  2016 SP2  |
|   VictoriaMetrics    |  v1.64.0   |
|        SQLite        |     --     |
|       OpenTSDB       |     --     |
|       KairosDB       |     --     |
|     TimescaleDB      |     --     |
|     TimescaleDB      |  Cluster   |
|       TDengine       |  2.2.0.2   |
|       TDengine       |   3.0.1    |
|      PI Archive      |    2016    |

# 3. 快速开始

## 3.1. 前置环境

为了使用 IoT Benchmark，你需要拥有：

1. Java 8
2. Maven：不建议使用镜像源，国内可以使用阿里云镜像源。
3. 合适版本的数据库

提示：
- CSV 的记录模式只能在 Linux 系统中使用，记录测试过程中的相关系统信息。
- 我们建议使用 MacOs 或 Linux 系统，本文以 MacOS 和 Linux 系统为例，如果使用 Windows 系统，请使用`conf`文件夹下的`benchmark.bat`脚本启动 benchmark。

## 3.2. 获取方式

在确保以上条件均以满足后，从 git 克隆源代码：

```
git clone https://github.com/apache/iotdb.git
```

默认的主分支是 master 分支，如果你想使用其他分支，请在克隆后进入项目根目录，并使用以下命令查看所有可用分支：

```
git branch -a
```

找到你希望工作的分支名后，使用以下命令切换到该分支：

```
git checkout [分支名]
```

使用如下命令通过 Maven 完成 iot-benchmark 的构建：

```
mvn clean package -Dmaven.test.skip=true
```
该命令会编译 iot-benchmark 的 core 模块，和所有其他相关的数据库。

## 3.3. 快速开始

提示：本文档中所有测试结果均出自如下环境：

```
CPU：I7-11700
内存：32G DDR4
系统盘：512G SSD (INTEL SSDPEKNU512GZ)
数据盘：2T HDD (WDC WD40EZAZ-00SF3B0)
```

在完成编译后，以 IoTDB v1.0 为例，**您需要首先在本机的 6667 端口启动相应版本的 IoTDB 服务**。（如果您对于使用 IoTDB 仍有疑问，请参照 [IoTDB_README.md](https://github.com/apache/iotdb/blob/master/README_ZH.md) 中的指引）。在成功启动 IoTDB 服务后，您可以进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0/iot-benchmark-iotdb-1.0`文件夹下，使用`./benchmark.sh`来启动对 IoTDB v1.0 的测试。我们推荐使用匹配的版本进行测试，以此达到最佳效果。

```

cd iotdb-1.0/target/iot-benchmark-iotdb-1.0/iot-benchmark-iotdb-1.0
./benchmark.sh
```

这将使用默认配置进行一次写入测试，测试启动后，如果执行无误，您将看到滚动的测试执行信息，其中部分信息如下：

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
  + okOperation：执行成功的不同操作的 Request/SQL 的数量
  + okPoint: 插入成功的数据点数或成功返回查询结果的数据点数
  + failOperation: 执行失败的不同操作的 Request/SQL 的数量
  + failPoint: 插入失败的数据点数（对于查询该值总为 0)
  + throughput: 等于```okPoint / Test elapsed time```
  + <a href = "https://y8dp9fjm8f.feishu.cn/file/boxcndtRvCh3qRNScNm8J5XERWf">参数详细说明</a>
+ 不同操作的毫秒级延迟统计
  + 其中```SLOWEST_THREAD``` 是客户端线程中的最大的累积操作时间长度

以上的全部信息都会被记录到运行设备的```logs```文件夹中。

配置文件存放在`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0/iot-benchmark-iotdb-1.0conf`下。当然，对于其他支持数据库您同样可以在相似的路径下找到其配置文件。编辑该文件来自定义测试的类型以及相关配置，**请注意，每次测试前您必须将配置文件中的 DB_SWITCH 参数更改为与待测数据库相匹配，其对应关系和可能取值如下所示：**

|        数据库        |    版本    |     对应子项目      |                                                  DB_SWITCH                                                   |
| :------------------: |:--------:| :-----------------: |:------------------------------------------------------------------------------------------------------------:|
|  IoTDB(1.0/1.1/1.3)  |   1.x    |      iotdb-1.x      | IoTDB-1x0-JDBC<br>IoTDB-1x0-SESSION_BY_TABLET<br>IoTDB-1x0-SESSION_BY_RECORD<br>IoTDB-1x0-SESSION_BY_RECORDS |
|       InfluxDB       |   1.x    |      influxdb       |                                                   InfluxDB                                                   |
|       InfluxDB       |   2.x    |    influxdb-2.0     |                                                 InfluxDB-2.x                                                 |
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

* 不同数据库使用说明详见 [被测数据库示例说明](./docs/DifferentTestDatabase.md)

同时，您还可以更改 BENCHMARK_WORK_MODE 参数调整 iot-benchmark 的运行模式，目前已经支持：

|    模式名称    |  BENCHMARK_WORK_MODE  | 模式内容                                                                        |
| :------------: | :-------------------: | :------------------------------------------------------------------------------ |
|  常规测试模式  |  testWithDefaultPath  | 支持多种读和写操作的混合负载                                                    |
|  生成数据模式  |   generateDataMode    | Benchmark 生成数据集到 FILE_PATH 路径中                                            |
| 正确性写入模式 | verificationWriteMode | 从 FILE_PATH 路径中加载数据集进行写入，目前支持 IoTDB v1.0 及更新的版本           |
| 正确性查询模式 | verificationQueryMode | 从 FILE_PATH 路径中加载数据集和数据库中进行比对，目前支持 IoTDB v1.0 及更新的版本 |

* 更多模式细节参考 [不同测试模式概述](./docs/DifferentTestMode.md)、[不同测试模式示例配置](./docs//DifferentTestModeConfig.md)
* 其他可变参数配置以及注解请见 [config.properties](configuration/conf/config.properties)，此处不做过多展开。

# 4. 相关文章

* [Benchmark Time Series Database with iot-benchmark for IoT Scenarios](https://arxiv.org/abs/1901.08304)

* [刘帅，乔颖，罗雄飞，赵怡婧，王宏安。时序数据库关键技术综述 [J]. 计算机研究与发展，2024, 61(3): 614-638. DOI: 10.7544/issn1000-1239.202330536](https://crad.ict.ac.cn/cn/article/doi/10.7544/issn1000-1239.202330536?viewType=HTML)
