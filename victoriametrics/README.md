VictoriaMetrics
---

# 测试环境
1. 本次测试使用Docker镜像进行正确性验证，拉取的是如下网址的latest的镜像

https://hub.docker.com/r/victoriametrics/victoria-metrics/

2. 环境配置过程
    1. `docker pull victoriametrics/victoria-metrics`
    2. `docker run -it --rm -v /path/to/victoria-metrics-data:/victoria-metrics-data -p 8428:8428 -d --name=victoria victoriametrics/victoria-metrics -retentionPeriod=30 -search.latencyOffset=1s -search.disableCache=true -search.maxPointsPerTimeseries=10000000`
    3. 请格外注意环境配置时的retentionPeriod参数的设计，该参数的单位为月，允许插入的时间序列范围为(当前月-retentionPeriod，当前月)
3. 查询基于Prometheus的API，参考https://blog.csdn.net/zhouwenjun0820/article/details/105823389
4. 目前暂时不支持后3种查询

# 配置文件修改(conf/config.properties)
1. 部分修改如下

```properties
DB_SWITCH=VictoriaMetrics
# 主机列表，如果有多个主机则使用英文逗号进行分割
# 其中如果是influxDB, opentsDB, kairosDB, ctsDB测试时需要完整路径，如"http://localhost:8086"
HOST=http://192.168.99.100
# 端口列表，需要和HOST数量一致，保持一一对应。如果有多个端口则使用英文逗号进行分割。
# IoTDB: 6667，TimescaleDB: 5432，TaosDB: 6030，InfluxDB：8086
# OpentsDB：4242，CtsDB:9200，KairosDB：8080 VictoriaMetrics: 8428
PORT=8428

# 由于retention time的设置，必须保证开始时间和计算后的结果时间在有效时间内
START_TIME=2021-01-01T00:00:00+08:00
```

2. [全部配置的文件](config.properties)

# 写入测试结果查看
1. 查看全部数据结果：`http://192.168.99.100:8428/api/v1/export?match={db="test"}`
2. 查看部分数据结果：`http://192.168.99.100:8428/api/v1/export?match={db="test", device="xxx", sensor="xxx"}`

示例结果
```
----------------------Main Configurations----------------------
DB_SWITCH: VictoriaMetrics
OPERATION_PROPORTION: 1:1:1:1:1:1:1:1:0:0:0
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 0:0:0:0:1:0
IS_CLIENT_BIND: true
CLIENT_NUMBER: 5
GROUP_NUMBER: 20
DEVICE_NUMBER: 5
SENSOR_NUMBER: 10
BATCH_SIZE_PER_WRITE: 10
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OUT_OF_ORDER: false
OUT_OF_ORDER_MODE: 1
OUT_OF_ORDER_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 7.98 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           656                 65600               0                   0                   8219.22             
PRECISE_POINT       607                 606                 0                   0                   75.93               
TIME_RANGE          648                 648                 0                   0                   81.19               
VALUE_RANGE         585                 584                 0                   0                   73.17               
AGG_RANGE           621                 621                 0                   0                   77.81               
AGG_VALUE           663                 663                 0                   0                   83.07               
AGG_RANGE_VALUE     589                 588                 0                   0                   73.67               
GROUP_BY            631                 631                 0                   0                   79.06               
LATEST_POINT        0                   0                   0                   0                   0.00                
RANGE_QUERY__DESC   0                   0                   0                   0                   0.00                
VALUE_RANGE_QUERY__DESC0                   0                   0                   0                   0.00                
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           9.00        0.60        1.56        3.18        7.63        12.97       17.59       21.19       32.27       54.21       51.73       1284.82     
PRECISE_POINT       5.22        0.32        0.69        1.26        3.49        7.83        12.15       14.46       19.72       81.00       59.97       706.73      
TIME_RANGE          5.87        0.34        0.76        1.31        3.63        8.59        13.65       17.23       28.87       70.56       60.09       893.48      
VALUE_RANGE         7.39        0.48        1.11        2.25        5.90        11.13       15.36       18.32       24.86       54.86       48.68       974.18      
AGG_RANGE           5.57        0.34        0.72        1.27        3.65        8.38        12.71       14.46       24.51       69.00       60.58       790.19      
AGG_VALUE           10.80       2.28        3.39        5.41        9.54        13.88       17.21       20.44       43.49       102.02      102.00      1570.65     
AGG_RANGE_VALUE     7.52        0.42        1.05        2.10        6.01        10.93       15.90       19.59       25.29       56.10       46.80       987.32      
GROUP_BY            10.46       2.27        3.35        4.89        9.27        14.35       17.97       21.80       31.93       55.90       55.28       1357.55     
LATEST_POINT        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
RANGE_QUERY__DESC   0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE_QUERY__DESC0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```