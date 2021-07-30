Benchmark influxdb
---
This project is using iotdb-benchmark to test influxdb

# environment
1. influxdb: 1.8.6-1
2. OS: windows

# config.properties
[使用配置文件](config.properties)

# config
```
----------------------Main Configurations----------------------
DB_SWITCH: InfluxDB
OPERATION_PROPORTION: 1:1:1:1:1:1:1:1:1:1:1
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
Create schema cost 0.02 second
Test elapsed time (not include schema creation): 16.60 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           1824                5472000             0                   0                   329673.94           
PRECISE_POINT       1831                1463                0                   0                   88.14               
TIME_RANGE          1776                59602               0                   0                   3590.87             
VALUE_RANGE         1871                62536               0                   0                   3767.63             
AGG_RANGE           1734                1425                0                   0                   85.85               
AGG_VALUE           1769                1759                0                   0                   105.98              
AGG_RANGE_VALUE     1790                1485                0                   0                   89.47               
GROUP_BY            1912                21455               0                   0                   1292.61             
LATEST_POINT        1841                1808                0                   0                   108.93              
RANGE_QUERY__DESC   1875                61658               0                   0                   3714.74             
VALUE_RANGE_QUERY__DESC1777                58972               0                   0                   3552.91             
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           30.63       10.88       15.55       18.02       25.41       35.65       43.04       53.86       91.61       484.29      597.10      3301.31     
PRECISE_POINT       14.26       0.71        3.49        5.96        13.29       17.81       28.24       34.81       49.22       70.51       97.71       1614.58     
TIME_RANGE          15.17       0.82        3.91        6.61        14.48       18.15       28.39       35.84       50.66       80.51       162.80      1506.59     
VALUE_RANGE         15.28       0.90        4.04        6.99        13.99       18.65       29.13       36.40       52.65       109.64      160.06      1846.49     
AGG_RANGE           14.76       0.79        3.71        6.63        13.95       17.91       27.73       34.84       47.83       87.21       105.82      1655.00     
AGG_VALUE           15.63       0.95        3.95        6.59        14.39       18.97       28.79       37.63       53.35       118.33      203.64      1631.59     
AGG_RANGE_VALUE     14.82       0.80        3.78        6.46        14.18       17.92       27.76       34.76       50.27       108.76      145.14      1741.13     
GROUP_BY            14.81       0.80        3.84        6.72        13.82       18.21       27.21       35.65       50.29       91.51       190.05      1680.38     
LATEST_POINT        14.91       0.76        3.63        6.09        13.54       18.17       29.30       37.28       53.76       101.07      160.93      1792.77     
RANGE_QUERY__DESC   14.96       0.76        3.93        6.69        13.77       17.93       27.40       35.16       53.58       107.22      112.44      1866.39     
VALUE_RANGE_QUERY__DESC15.56       1.12        4.10        6.94        14.43       18.49       29.46       37.13       51.59       136.56      161.32      1608.77     
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```