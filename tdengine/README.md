# Benchmark for TDengine

## The Environment of Test
Firstly, you need to install TDengine-server on the server machine and change the hostname to `tdengine` in`hostname` file.
Then restart the server machine.


Secondly, you need to add a static routing of the server's ip address and hostname to `hosts` file on the test machine.
```properties
192.168.0.11    tdengine
```
Lastly, You need to install the TDengine-client on the test machine.
## Configuration of Benchmark
There is a [sample configuration file](./config.properties).
## The Result of Test
```
----------------------Main Configurations----------------------
CREATE_SCHEMA=false
START_TIME=2018-9-20T00:00:00+08:00
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1
BATCH_SIZE_PER_WRITE=10
IS_CLIENT_BIND=true
LOOP=1000
IS_OUT_OF_ORDER=false
IS_REGULAR_FREQUENCY=true
GROUP_NUMBER=20
QUERY_INTERVAL=250000
SENSOR_NUMBER=10
RESULT_PRECISION=0.1%
POINT_STEP=5000
CLIENT_NUMBER=5
SG_STRATEGY=mod
REAL_INSERT_RATE=1.0
OUT_OF_ORDER_MODE=0
DBConfig=
  DB_SWITCH=TDengine
  HOST=[fit11]
  PORT=[6030]
  USERNAME=root
  PASSWORD=taosdata
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=false
BENCHMARK_WORK_MODE=testWithDefaultPath
OP_MIN_INTERVAL=0
OPERATION_PROPORTION=0:1:1:1:1:1:1:1:1:1:1
DEVICE_NUMBER=5
OUT_OF_ORDER_RATIO=0.5
BENCHMARK_CLUSTER=false
IS_DELETE_DATA=false
IS_SENSOR_TS_ALIGNMENT=true
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 233.94 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           0                   0                   0                   0                   0.00                
PRECISE_POINT       532                 527                 0                   0                   2.25                
TIME_RANGE          489                 24938               0                   0                   106.60              
VALUE_RANGE         495                 25242               0                   0                   107.90              
AGG_RANGE           519                 519                 0                   0                   2.22                
AGG_VALUE           461                 461                 0                   0                   1.97                
AGG_RANGE_VALUE     514                 514                 0                   0                   2.20                
GROUP_BY            491                 6625                0                   0                   28.32               
LATEST_POINT        514                 514                 0                   0                   2.20                
RANGE_QUERY_DESC    482                 24578               0                   0                   105.06              
VALUE_RANGE_QUERY_DESC503                 25651               0                   0                   109.65              
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT       264.71      173.58      228.16      229.82      232.59      241.92      304.70      366.21      869.92      2605.45     2125.49     30132.31    
TIME_RANGE          268.92      226.36      229.68      231.56      235.12      251.71      309.87      372.38      914.31      1215.76     1213.33     29016.32    
VALUE_RANGE         272.98      224.50      228.98      231.05      233.55      243.98      302.89      367.54      962.22      2684.65     2424.54     29182.24    
AGG_RANGE           196.84      168.74      171.36      172.87      174.63      181.54      226.29      265.82      570.26      1270.12     1136.97     22478.98    
AGG_VALUE           196.21      168.28      171.42      172.94      175.18      181.22      232.12      266.52      566.86      927.67      900.91      20892.08    
AGG_RANGE_VALUE     197.06      168.81      171.35      172.98      174.71      180.11      207.67      264.03      543.92      3581.53     2664.52     22335.86    
GROUP_BY            199.84      169.29      172.21      173.60      175.61      183.49      234.11      273.43      581.25      1800.64     1495.77     23396.05    
LATEST_POINT        193.71      166.79      171.03      172.59      174.68      181.12      219.96      273.66      542.22      909.61      900.14      23351.74    
RANGE_QUERY_DESC    264.52      226.29      229.61      230.98      234.44      255.24      305.27      370.94      655.00      1012.37     1006.99     26868.51    
VALUE_RANGE_QUERY_DESC268.29      225.66      229.35      231.09      234.11      246.85      301.83      360.08      678.09      4306.46     3183.36     30923.14    
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```