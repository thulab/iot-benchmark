# Benchmark for KairosDB
## Installation of KairosDB
1. Download and decompress the installation file of KairosDB with the following command:
```shell
sudo tar -zxvf kairosdb-version.tar.gz -C /path/to/kairosdb
```
2. Start KairosDB in background with the following command:
```shell
cd /path/to/kairosdb
./bin/kairosdb.sh start
```
## Configuration of Benchmark
Because of the rule in KairosDB, the `QUERY_LOWER_VALUE` must be greater than or equal to `0`. You should modify the following parameter in `conf/config.properties`.
```properties
QUERY_LOWER_VALUE=0
```
There is a [sample configuration file](./config.properties).
## The Result of Test
```
----------------------Main Configurations----------------------
BENCHMARK_WORK_MODE=testWithDefaultPath
RESULT_PRECISION=0.1%
DBConfig=
  DB_SWITCH=KairosDB
  HOST=[192.168.174.101]
  PORT=[8080]
  USERNAME=root
  PASSWORD=root
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=false
BENCHMARK_CLUSTER=false
OPERATION_PROPORTION=0:1:1:1:1:1:1:1:1:1:1
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
IS_DELETE_DATA=true
CREATE_SCHEMA=true
IS_CLIENT_BIND=true
CLIENT_NUMBER=5
GROUP_NUMBER=20
SG_STRATEGY=mod
DEVICE_NUMBER=5
REAL_INSERT_RATE=1.0
SENSOR_NUMBER=10
IS_SENSOR_TS_ALIGNMENT=true
BATCH_SIZE_PER_WRITE=10
LOOP=1000
POINT_STEP=5000
OP_INTERVAL=0
QUERY_INTERVAL=250000
IS_OUT_OF_ORDER=false
OUT_OF_ORDER_MODE=0
OUT_OF_ORDER_RATIO=0.5
IS_REGULAR_FREQUENCY=true
START_TIME=2018-9-20T00:00:00+08:00
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 23.29 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           0                   0                   0                   0                   0.00                
PRECISE_POINT       462                 462                 0                   0                   19.84               
TIME_RANGE          438                 22338               0                   0                   959.19              
VALUE_RANGE         483                 153                 0                   0                   6.57                
AGG_RANGE           425                 426                 0                   0                   18.29               
AGG_VALUE           460                 3                   0                   0                   0.13                
AGG_RANGE_VALUE     431                 3                   0                   0                   0.13                
GROUP_BY            473                 6149                0                   0                   264.04              
LATEST_POINT        459                 462                 0                   0                   19.84               
RANGE_QUERY_DESC    0                   0                   433                 0                   0.00                
VALUE_RANGE_QUERY_DESC0                   0                   458                 0                   0.00                
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT       11.20       2.47        3.22        3.96        7.69        14.29       22.12       30.88       43.54       127.88      127.47      1329.46     
TIME_RANGE          11.60       2.87        3.60        4.21        7.26        14.25       27.52       33.33       53.30       61.41       60.97       1147.63     
VALUE_RANGE         10.83       2.71        3.43        4.08        6.92        14.18       23.23       31.59       41.42       68.34       64.30       1249.42     
AGG_RANGE           11.42       2.66        3.34        4.37        6.78        13.69       23.33       33.80       63.70       95.89       87.98       1214.00     
AGG_VALUE           160.27      21.79       71.73       90.71       126.63      198.72      286.34      361.39      526.07      791.49      731.11      16078.67    
AGG_RANGE_VALUE     11.53       2.72        3.55        4.36        8.19        14.05       24.24       32.71       49.00       95.40       83.73       1147.16     
GROUP_BY            10.25       2.78        3.49        4.25        6.98        13.19       20.28       26.28       45.35       73.56       69.57       1070.28     
LATEST_POINT        12.04       2.79        3.52        4.33        7.34        13.65       24.31       35.03       95.24       140.48      126.53      1324.65     
RANGE_QUERY_DESC    0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE_QUERY_DESC0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```