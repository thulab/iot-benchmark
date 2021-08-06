Benchmark influxdb
---
This project is using iotdb-benchmark to test influxdb 2

# environment
1. influxdb: 2.0.7

# database setup
1. visit http://{ip}:8086/ to set up user
    1. username: admin
    2. password: 12345678
    3. org: admin
    4. bucket: admin

# config
1. This is [config.properties](config.properties)
2. This is [config.yaml for InfluxDB v2.0](config.yaml)
3. [语法参考](https://docs.influxdata.com/influxdb/v2.0/reference/flux/)

# test result
```
----------------------Main Configurations----------------------
DB_SWITCH: InfluxDB-2.0
OPERATION_PROPORTION: 1:1:1:1:1:1:1:1:1:1:1
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
Create schema cost 0.04 second
Test elapsed time (not include schema creation): 14.15 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           478                 47800               0                   0                   3378.25             
PRECISE_POINT       462                 183                 0                   0                   12.93               
TIME_RANGE          438                 7150                0                   0                   505.32              
VALUE_RANGE         483                 7769                0                   0                   549.07              
AGG_RANGE           425                 179                 0                   0                   12.65               
AGG_VALUE           460                 419                 0                   0                   29.61               
AGG_RANGE_VALUE     431                 179                 0                   0                   12.65               
GROUP_BY            473                 203                 0                   0                   14.35               
LATEST_POINT        459                 413                 0                   0                   29.19               
RANGE_QUERY__DESC   433                 6893                0                   0                   487.16              
VALUE_RANGE_QUERY__DESC458                 7639                0                   0                   539.88              
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           2.65        0.36        0.64        0.76        0.86        1.02        1.17        1.46        69.42       254.12      232.85      323.81      
PRECISE_POINT       13.63       7.49        9.17        10.68       12.62       15.54       18.57       20.68       27.37       114.62      89.66       1431.18     
TIME_RANGE          13.60       8.00        9.40        10.98       12.66       15.36       18.95       21.38       27.85       42.81       39.92       1413.08     
VALUE_RANGE         15.30       8.33        10.64       12.34       14.31       17.69       20.51       22.69       28.77       39.57       37.29       1588.68     
AGG_RANGE           14.06       8.11        9.75        11.27       13.06       15.71       19.76       21.85       25.62       42.43       40.70       1316.42     
AGG_VALUE           16.81       9.68        11.97       13.58       15.71       18.89       23.16       25.60       33.04       44.15       44.15       1675.84     
AGG_RANGE_VALUE     15.32       9.19        11.13       12.54       14.50       17.21       20.60       22.51       26.43       51.10       45.37       1430.33     
GROUP_BY            14.83       8.31        10.30       11.76       13.89       16.25       18.94       21.64       27.82       97.27       95.79       1461.56     
LATEST_POINT        15.19       8.26        10.29       11.87       13.63       16.68       20.33       23.22       56.84       65.79       65.77       1625.66     
RANGE_QUERY__DESC   14.88       8.16        10.30       11.97       13.76       16.75       20.20       22.60       26.96       107.33      90.23       1421.37     
VALUE_RANGE_QUERY__DESC16.40       9.51        11.71       13.35       15.63       18.54       22.11       24.06       28.92       37.47       35.63       1609.34     
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```