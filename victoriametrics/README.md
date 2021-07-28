VictoriaMetrics
---

# 测试环境
1. 本次测试使用Docker镜像进行正确性验证，拉取的是如下网址的latest的镜像

https://hub.docker.com/r/victoriametrics/victoria-metrics/

2. 环境配置过程
    1. `docker pull victoriametrics/victoria-metrics`
    2. `docker run -it --rm -v /path/to/victoria-metrics-data:/victoria-metrics-data -p 8428:8428 -d --name=victoria victoriametrics/victoria-metrics -retentionPeriod=30 -search.latencyOffset=1s -search.disableCache=true`
    3. 请格外注意环境配置时的retentionPeriod参数的设计，该参数的单位为月，允许插入的时间序列范围为(当前月-retentionPeriod，当前月)

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
OPERATION_PROPORTION: 1:0:0:0:0:0:0:0:0:0:0
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 0:0:0:0:1:0
IS_CLIENT_BIND: true
CLIENT_NUMBER: 20
GROUP_NUMBER: 20
DEVICE_NUMBER: 20
SENSOR_NUMBER: 300
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
Test elapsed time (not include schema creation): 71.97 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           20000               60000000            0                   0                   833640.66           
PRECISE_POINT       0                   0                   0                   0                   0.00                
TIME_RANGE          0                   0                   0                   0                   0.00                
VALUE_RANGE         0                   0                   0                   0                   0.00                
AGG_RANGE           0                   0                   0                   0                   0.00                
AGG_VALUE           0                   0                   0                   0                   0.00                
AGG_RANGE_VALUE     0                   0                   0                   0                   0.00                
GROUP_BY            0                   0                   0                   0                   0.00                
LATEST_POINT        0                   0                   0                   0                   0.00                
RANGE_QUERY__DESC   0                   0                   0                   0                   0.00                
VALUE_RANGE_QUERY__DESC0                   0                   0                   0                   0.00                
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           71.37       4.00        46.23       55.84       67.03       82.35       99.28       113.04      155.83      368.31      417.49      71858.81    
PRECISE_POINT       0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
TIME_RANGE          0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE         0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_RANGE           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_VALUE           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_RANGE_VALUE     0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
GROUP_BY            0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
LATEST_POINT        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
RANGE_QUERY__DESC   0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE_QUERY__DESC0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```