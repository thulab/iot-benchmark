Benchmark cnosdb
---
This project is using iot-benchmark to test cnosdb

# 1. environment
1. cnosdb

# 2. config
[Demo config](config.properties)

# 3. test result
```
----------------------Main Configurations----------------------
########### Test Mode ###########
BENCHMARK_WORK_MODE=testWithDefaultPath
########### Database Connection Information ###########
DOUBLE_WRITE=false
DBConfig=
  DB_SWITCH=CnosDB
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
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1:1
CLIENT_NUMBER=20
LOOP=1000
BATCH_SIZE_PER_WRITE=10
DEVICE_NUM_PER_WRITE=1
START_TIME=2022-01-01T00:00:00+08:00
POINT_STEP=5000
OP_MIN_INTERVAL=0
OP_MIN_INTERVAL_RANDOM=false
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
ENCODINGS=RLE/TS_2DIFF/TS_2DIFF/GORILLA/GORILLA/DICTIONARY
COMPRESSOR=LZ4
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
VECTOR=true

---------------------------------------------------------------
main measurements:
Create schema cost 0.03 second
Test elapsed time (not include schema creation): 710.18 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)      
INGESTION                1668                     5004000                  0                        0                        7046.06                  
PRECISE_POINT            1688                     24                       0                        0                        0.03                     
TIME_RANGE               1609                     155044                   0                        0                        218.32                   
VALUE_RANGE              1724                     166220                   0                        0                        234.05                   
AGG_RANGE                1634                     1650                     0                        0                        2.32                     
AGG_VALUE                1589                     1603                     0                        0                        2.26                     
AGG_RANGE_VALUE          1618                     1631                     0                        0                        2.30                     
GROUP_BY                 1654                     21277                    0                        0                        29.96                    
LATEST_POINT             1790                     1825                     0                        0                        2.57                     
RANGE_QUERY_DESC         1706                     164454                   0                        0                        231.57                   
VALUE_RANGE_QUERY_DESC   1686                     162880                   0                        0                        229.35                   
GROUP_BY_DESC            1634                     20921                    0                        0                        29.46                    
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION                488.67      25.38       297.81      372.06      472.59      585.16      686.56      780.46      931.55      2365.09     2608.27     48215.75    
PRECISE_POINT            703.79      4.50        509.56      608.36      688.93      811.19      915.26      983.28      1144.30     1376.49     1404.58     67672.06    
TIME_RANGE               703.09      3.94        496.42      610.65      691.23      809.82      923.24      997.29      1137.73     1305.43     1449.56     64305.71    
VALUE_RANGE              710.36      3.61        508.36      613.90      706.27      812.29      924.29      977.06      1114.60     1413.82     1454.81     68056.47    
AGG_RANGE                710.81      3.46        506.80      615.11      701.39      816.73      925.90      993.49      1148.46     1300.30     1473.81     75751.07    
AGG_VALUE                714.49      4.99        515.91      616.42      702.66      823.51      925.31      996.28      1145.38     1333.55     1415.87     65565.50    
AGG_RANGE_VALUE          719.29      2.92        529.65      623.54      709.55      823.32      927.30      997.72      1133.30     1227.32     1261.44     77830.08    
GROUP_BY                 788.29      6.62        560.39      653.02      771.46      915.59      1046.47     1153.96     1306.75     1479.48     1652.89     79228.42    
LATEST_POINT             709.25      3.17        496.80      615.54      701.74      819.30      944.59      1002.03     1136.95     1239.04     1249.00     75956.11    
RANGE_QUERY_DESC         699.29      3.46        496.19      604.62      685.97      805.92      911.87      965.89      1128.06     1283.03     1295.40     69943.04    
VALUE_RANGE_QUERY_DESC   709.52      5.48        512.47      617.11      701.39      813.19      917.91      977.22      1141.64     1369.16     1411.72     68471.95    
GROUP_BY_DESC            787.70      5.28        560.25      648.31      773.14      921.28      1048.09     1147.17     1265.12     1528.68     1603.18     74526.22    
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```