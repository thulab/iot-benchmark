Benchmark victoriaMetrics
---

# environment(eg. docker)
1. docker image: https://hub.docker.com/r/victoriametrics/victoria-metrics/

2. The process of environment configuration
    1. `docker pull victoriametrics/victoria-metrics`
    2. `docker run -it --rm -v /path/to/victoria-metrics-data:/victoria-metrics-data -p 8428:8428 -d --name=victoria victoriametrics/victoria-metrics -retentionPeriod=30 -search.latencyOffset=1s -search.disableCache=true -search.maxPointsPerTimeseries=10000000`
    3. Please pay special attention to the design of the retentionPeriod parameter during environment configuration. The unit of this parameter is month, and the range of time series allowed to be inserted is (current month-retentionPeriod, current month)
3. Query API based on Prometheus: https://blog.csdn.net/zhouwenjun0820/article/details/105823389

# config
1. [Demo config](config.properties)

# test result
1. 查看全部数据结果：`${HOST}:${PORT}/api/v1/export?match={db="${DB_NAME}"}`
2. 查看部分数据结果：`${HOST}:${PORT}/api/v1/export?match={db="${DB_NAME}", device="xxx", sensor="xxx"}`

```
----------------------Main Configurations----------------------
DB_SWITCH: VictoriaMetrics
OPERATION_PROPORTION: 1:1:1:1:1:1:1:1:1:1:1
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:0
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
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 23.35 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           478                 95600               0                   0                   4093.81             
PRECISE_POINT       462                 315                 0                   0                   13.49               
TIME_RANGE          438                 318                 0                   0                   13.62               
VALUE_RANGE         483                 354                 0                   0                   15.16               
AGG_RANGE           425                 317                 0                   0                   13.57               
AGG_VALUE           460                 331                 0                   0                   14.17               
AGG_RANGE_VALUE     431                 313                 0                   0                   13.40               
GROUP_BY            473                 352                 0                   0                   15.07               
LATEST_POINT        459                 337                 0                   0                   14.43               
RANGE_QUERY_DESC    433                 317                 0                   0                   13.57               
VALUE_RANGE_QUERY_DESC458                 338                 0                   0                   14.47               
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           22.92       1.95        4.84        8.72        18.79       35.38       46.39       52.21       71.87       90.49       87.38       2398.85     
PRECISE_POINT       16.17       0.64        1.44        2.99        9.39        24.86       40.86       47.20       72.02       92.07       91.82       1694.40     
TIME_RANGE          17.27       0.95        1.74        3.30        10.16       27.63       43.87       53.58       64.03       94.77       90.97       1654.97     
VALUE_RANGE         21.96       1.18        3.01        5.34        15.70       34.35       49.84       55.97       80.59       104.17      103.41      2398.72     
AGG_RANGE           15.71       0.65        1.71        2.87        8.18        25.33       39.64       48.88       64.22       89.84       86.06       1472.52     
AGG_VALUE           29.76       1.49        4.37        10.63       26.65       41.84       60.73       69.19       88.93       120.80      118.68      2924.50     
AGG_RANGE_VALUE     22.44       1.37        3.00        6.17        17.55       33.89       47.00       55.58       80.33       146.34      138.35      2408.02     
GROUP_BY            31.55       1.15        6.04        11.70       28.67       46.01       64.00       75.06       95.24       128.98      124.33      3276.15     
LATEST_POINT        31.70       1.83        5.66        10.82       28.70       42.88       60.74       76.76       140.91      166.58      166.47      3419.06     
RANGE_QUERY_DESC    18.28       0.67        1.88        3.74        11.67       27.75       44.54       55.34       77.63       96.51       91.89       1987.14     
VALUE_RANGE_QUERY_DESC21.87       1.30        3.12        6.51        16.77       32.46       47.23       55.88       76.17       135.72      123.52      2115.82     
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```