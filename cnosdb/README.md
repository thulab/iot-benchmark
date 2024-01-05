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
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1:0
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
Create schema cost 0.04 second
Test elapsed time (not include schema creation): 61.48 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation                okOperation              okPoint                  failOperation            failPoint                throughput(point/s)
INGESTION                1824                     5472000                  0                        0                        88997.41
PRECISE_POINT            1831                     18                       0                        0                        0.29
TIME_RANGE               1776                     86142                    0                        0                        1401.03
VALUE_RANGE              1871                     90330                    0                        0                        1469.14
AGG_RANGE                1734                     1744                     0                        0                        28.36
AGG_VALUE                1769                     1780                     0                        0                        28.95
AGG_RANGE_VALUE          1790                     1802                     0                        0                        29.31
GROUP_BY                 1912                     24713                    0                        0                        401.94
LATEST_POINT             1841                     1872                     0                        0                        30.45
RANGE_QUERY_DESC         1875                     90732                    0                        0                        1475.68
VALUE_RANGE_QUERY_DESC   1777                     85690                    0                        0                        1393.67
GROUP_BY_DESC            0                        0                        0                        0                        0.00
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation                AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION                78.37       13.21       51.99       54.87       58.67       63.00       66.95       70.70       1782.16     1853.94     1857.63     7942.20
PRECISE_POINT            53.11       2.61        42.40       45.68       53.31       58.52       65.14       69.73       76.69       85.30       92.04       5647.39
TIME_RANGE               55.16       3.79        44.30       48.38       55.22       60.72       66.86       71.93       78.31       85.23       89.00       5639.34
VALUE_RANGE              55.13       3.42        44.22       48.04       55.55       60.65       66.54       71.76       77.79       84.87       90.32       5943.05
AGG_RANGE                54.61       2.95        43.83       47.67       54.60       60.17       66.51       71.11       77.38       82.44       89.83       5968.74
AGG_VALUE                54.77       3.14        44.37       48.23       54.99       59.88       65.52       71.27       79.16       84.40       84.84       5867.98
AGG_RANGE_VALUE          54.78       3.14        44.50       48.27       55.22       59.64       64.86       71.70       78.66       86.26       91.38       6231.83
GROUP_BY                 66.78       3.02        55.59       60.11       66.73       72.50       78.79       83.99       91.05       97.50       103.94      7539.02
LATEST_POINT             57.53       3.31        44.61       48.47       55.44       60.51       67.18       73.03       272.03      274.39      274.43      6280.19
RANGE_QUERY_DESC         55.22       3.15        44.44       48.74       55.32       60.39       66.87       71.66       77.81       86.94       87.38       6380.15
VALUE_RANGE_QUERY_DESC   55.13       3.41        44.40       48.62       55.28       60.37       66.55       71.92       77.35       83.76       88.17       5787.75
GROUP_BY_DESC            0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```