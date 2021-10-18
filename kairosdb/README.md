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
IS_DELETE_DATA=false
CREATE_SCHEMA=false
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
Test elapsed time (not include schema creation): 9.64 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           0                   0                   0                   0                   0.00                
PRECISE_POINT       532                 532                 0                   0                   55.21               
TIME_RANGE          489                 24939               0                   0                   2587.94             
VALUE_RANGE         29                  102                 0                   0                   10.58               
AGG_RANGE           519                 519                 0                   0                   53.86               
AGG_VALUE           29                  1                   0                   0                   0.10                
AGG_RANGE_VALUE     32                  1                   0                   0                   0.10                
GROUP_BY            491                 6383                0                   0                   662.37              
LATEST_POINT        514                 514                 0                   0                   53.34               
RANGE_QUERY_DESC    482                 24582               0                   0                   2550.89             
VALUE_RANGE_QUERY_DESC39                  459                 0                   0                   47.63               
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT       10.69       3.19        4.12        5.92        8.79        12.45       19.02       24.33       36.62       170.15      126.51      1250.41     
TIME_RANGE          12.09       3.52        4.53        6.54        10.04       15.17       21.43       27.08       46.99       79.90       76.27       1300.06     
VALUE_RANGE         19.89       5.18        5.24        11.12       14.62       26.10       38.86       39.19       49.63       47.58       47.35       147.16      
AGG_RANGE           11.25       3.35        4.52        6.24        9.12        13.14       20.51       24.85       38.51       83.24       76.69       1393.22     
AGG_VALUE           258.73      97.18       98.95       168.64      219.92      253.94      492.78      553.35      1027.55     944.99      935.82      1697.52     
AGG_RANGE_VALUE     19.64       9.00        9.76        11.32       15.45       22.71       31.40       36.89       80.33       71.23       70.22       176.03      
GROUP_BY            10.94       3.33        4.39        5.81        8.57        12.68       19.36       24.92       51.28       131.41      112.85      1242.27     
LATEST_POINT        14.49       3.53        4.50        6.47        9.31        12.69       20.74       28.42       38.88       380.66      380.64      1722.99     
RANGE_QUERY_DESC    11.11       3.35        4.60        6.31        9.17        12.72       19.19       24.64       41.69       78.83       76.07       1218.01     
VALUE_RANGE_QUERY_DESC22.88       8.62        9.28        12.74       17.39       25.59       37.71       57.09       158.96      133.41      130.57      235.26      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```