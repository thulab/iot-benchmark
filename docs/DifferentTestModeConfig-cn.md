![](https://img.shields.io/badge/platform-MacOS%20%7C%20Linux%20%7C%20Windows-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)

** This document shows the specific configuration and execution of several test scenarios given in README.md **
** 该文档展示了README.md中给出的几种测试场景的具体配置以及具体执行情况 **

# 1. 常规测试模式之查询(单数据库，不使用系统记录) Benchmark的配置 

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

## 1.1. Benchmark的启动 

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

## 1.2. Benchmark的执行

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



# 2. 6.2.2 常规测试模式之读写混合模式（单数据库） Benchmark的配置

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

## 2.1. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

## 2.2. Benchmark的执行

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




# 3. 6.2.3 常规测试模式之读写混合模式（单数据库，查询最近写入数据） Benchmark的配置

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

## 3.1. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

## 3.2. Benchmark的执行

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




# 4. 6.2.4 常规测试模式之使用系统记录（单数据库） Benchmark的配置

假设您的 IoTDB 服务器 IP 是 192.168.130.9，并且您安装了 iot-benchmark 的测试客户端服务器已授权访问 IoTDB 服务器。

当前版本的信息记录依赖于 iostat。请确保 iostat 已安装在 IoTDB 服务器中。

之后配置```config.properties```
假设您使用的参数与[简单指引](../#61-常规测试模式之写入单数据库)中的参数相同。您应该添加的新参数是 TEST_DATA_PERSISTENCE 和 MONITOR_INTERVAL，例如：

```properties
TEST_DATA_PERSISTENCE=CSV
MONITOR_INTERVAL=0
```

> 1. TEST_DATA_PERSISTENCE=CSV 表示测试结果保存到CSV中。
> 2. INTERVAL=0 表示服务器信息记录的间隔最小为 2 秒。 如果您设置 INTERVAL=n，那么间隔将为 n+2 秒，因为记录过程至少需要2秒。在进行长时间测试时，您可能希望将 INTERVAL 设置得更长。

## 4.1. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

其他后续过程和上文所述类似，最后生成的相关文件会存放在```data```目录下。




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




# 5. 6.2.5 常规测试模式之测试过程持久化（单数据库） Benchmark配置

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





# 6. 6.2.6 生成数据模式 Benchmark的配置

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

## 6.1. Benchmark的启动

您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

## 6.2. Benchmark的执行

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




# 7. 6.2.7 正确性写入模式（单数据库，外部数据集） Benchmark的配置

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

## 7.1. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

## 7.2. Benchmark的执行

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




# 8. 6.2.8 正确性单点查询模式（单数据库，外部数据集） Benchmark的配置

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
 
## 8.1. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务。

之后您进入到`iot-benchmark/iotdb-1.0/target/iot-benchmark-iotdb-1.0`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

## 8.2. Benchmark的执行
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


# 9. 6.2.9 双数据库模式 Benchmark配置

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




# 10. 6.2.10 常规测试模式之写入(双数据库) Benchmark的配置

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

## 10.1. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务，并且在5432端口启动TimescaleDB服务

之后您进入到`iot-benchmark/verfication/target/iot-benchmark-verification`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

## 10.2. Benchmark的执行

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




# 11. 6.2.11 正确性单点查询模式（双数据库比较） Benchmark的配置

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

## 11.1. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务，并且在5432端口启动TimescaleDB服务

之后您进入到`iot-benchmark/verfication/target/iot-benchmark-verification`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

## 11.2. Benchmark的执行

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




# 12. 6.2.12 正确性功能查询模式（双数据库比较） Benchmark的配置

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

## 12.1. Benchmark的启动

在启动测试之前，您需要在本机的6667端口启动IoTDB服务，并且在5432端口启动TimescaleDB服务

之后您进入到`iot-benchmark/verfication/target/iot-benchmark-verification`中运行如下命令来启动Benchmark(目前仅Unix/OS X系统中执行如下脚本)：

```sh
> ./benchmark.sh
```

## 12.2. Benchmark的执行
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
