Verification
---

# environment
1. IoTDB v0.12
2. InfluxDB v1.x

# config
[Demo config](config.properties)

其中需要配置

```
################ Benchmark：双写模式 ####################
IS_DOUBLE_WRITE=true
# 另一个写入的数据库，目前的格式为{name}{-version}{-insert mode}(注意-号)其全部参考值参见README文件
ANOTHER_DB_SWITCH=InfluxDB
# 另一个写入的数据库的主机
ANOTHER_HOST=http://127.0.0.1
# 另一个写入的数据库的端口
ANOTHER_PORT=8086
# 另一个写入的数据库的用户名
ANOTHER_USERNAME=root
# 另一个写入的数据库的密码，如果为多个数据库，则要求保持一致
ANOTHER_PASSWORD=root
# 另一个写入的数据库的名称
ANOTHER_DB_NAME=test
# 另一个数据库认证使用的Token，目前仅限于InfluxDB 2.0使用
ANOTHER_TOKEN=token
```

# test result

```
----------------------Main Configurations----------------------
DB_SWITCH: IoTDB-012-SESSION_BY_TABLET,InfluxDB
OPERATION_PROPORTION: 1:1:1:1:1:1:1:1:1:1:1
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:1
IS_CLIENT_BIND: true
CLIENT_NUMBER: 5
GROUP_NUMBER: 20
DEVICE_NUMBER: 5
SENSOR_NUMBER: 20
BATCH_SIZE_PER_WRITE: 10
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OUT_OF_ORDER: false
OUT_OF_ORDER_MODE: 0
OUT_OF_ORDER_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.01 second
Test elapsed time (not include schema creation): 19.27 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           956                 191200              0                   0                   9924.71             
PRECISE_POINT       924                 0                   0                   0                   0.00                
TIME_RANGE          876                 27                  0                   0                   1.40                
VALUE_RANGE         966                 43                  0                   0                   2.23                
AGG_RANGE           850                 426                 0                   0                   22.11               
AGG_VALUE           920                 919                 0                   0                   47.70               
AGG_RANGE_VALUE     862                 432                 0                   0                   22.42               
GROUP_BY            946                 6189                0                   0                   321.26              
LATEST_POINT        918                 907                 0                   0                   47.08               
RANGE_QUERY_DESC    866                 11                  0                   0                   0.57                
VALUE_RANGE_QUERY_DESC916                 119                 0                   0                   6.18                
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           10.79       0.34        0.50        0.62        5.28        17.65       22.88       29.34       75.92       153.85      145.85      2264.69     
PRECISE_POINT       8.80        0.89        1.20        1.51        2.53        14.69       20.83       26.12       64.79       144.71      139.25      2003.94     
TIME_RANGE          8.72        0.88        1.21        1.53        2.48        14.21       21.32       31.35       54.64       157.38      146.58      1937.67     
VALUE_RANGE         8.88        0.88        1.25        1.53        2.63        14.68       22.18       30.64       55.03       140.77      109.24      1840.67     
AGG_RANGE           9.06        1.05        1.38        1.70        2.73        14.75       21.60       31.44       57.01       136.47      134.45      1734.16     
AGG_VALUE           9.63        1.35        1.88        2.36        3.39        15.77       21.97       27.86       49.40       93.04       82.99       2007.27     
AGG_RANGE_VALUE     8.92        0.98        1.54        1.83        2.79        15.12       20.60       29.42       51.23       107.11      101.32      1818.84     
GROUP_BY            9.21        1.22        1.57        1.94        2.81        15.42       21.72       31.97       48.08       129.33      106.57      1953.39     
LATEST_POINT        9.54        0.87        1.19        1.46        2.68        15.26       22.86       30.93       65.48       139.63      127.04      2014.09     
RANGE_QUERY_DESC    9.29        0.77        1.28        1.56        2.69        15.19       23.27       33.31       52.88       129.36      120.59      1835.79     
VALUE_RANGE_QUERY_DESC9.09        0.92        1.24        1.55        2.51        14.83       22.41       31.79       57.93       133.38      124.01      1816.77     
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```