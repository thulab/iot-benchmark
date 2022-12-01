# Benchmark for PIArchive
## The Environment of Test
You need to firstly install the PI JDBC on the test machine, if you want to start this test. Besides, you need to add the jar file of PI JDBC to `Libraries` of the project. The path of jar file is `%PIHOME%\JDBC\PIJDBCDriver.jar`.  
The module of PI will not compile in default. Therefore, you need to cancel the annotation of PI module in `pom.xml`, if you want to compile the module.
```xml
<modules>
        ...
        <module>verification</module>
        <!-- cancel this annotation -->
<!--        <module>pi</module>-->
    </modules>
```
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
SENSOR_NUMBER=100
RESULT_PRECISION=0.1%
POINT_STEP=5000
CLIENT_NUMBER=5
SG_STRATEGY=mod
REAL_INSERT_RATE=1.0
OUT_OF_ORDER_MODE=0
DBConfig=
  DB_SWITCH=PIArchive
  HOST=[127.0.0.1]
  PORT=[6667]
  USERNAME=root
  PASSWORD=root
  DB_NAME=test
  TOKEN=token
DOUBLE_WRITE=false
BENCHMARK_WORK_MODE=testWithDefaultPath
OP_MIN_INTERVAL=0
OPERATION_PROPORTION=0:1:1:1:1:1:1:0:0:1:1
DEVICE_NUMBER=1
OUT_OF_ORDER_RATIO=0.5
BENCHMARK_CLUSTER=false
IS_DELETE_DATA=false
IS_SENSOR_TS_ALIGNMENT=true
---------------------------------------------------------------
main measurements:
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 19.56 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           0                   0                   0                   0                   0.00                
PRECISE_POINT       656                 635                 0                   0                   32.47               
TIME_RANGE          607                 31563               0                   0                   1613.96             
VALUE_RANGE         648                 34098               0                   0                   1743.58             
AGG_RANGE           585                 585                 0                   0                   29.91               
AGG_VALUE           621                 621                 0                   0                   31.75               
AGG_RANGE_VALUE     663                 663                 0                   0                   33.90               
GROUP_BY            0                   0                   0                   0                   0.00                
LATEST_POINT        0                   0                   0                   0                   0.00                
RANGE_QUERY_DESC    589                 30557               0                   0                   1562.51             
VALUE_RANGE_QUERY_DESC631                 33700               0                   0                   1723.23             
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT       11.55       5.85        7.78        9.23        11.05       13.45       15.62       17.70       20.51       24.94       24.29       1752.68     
TIME_RANGE          11.75       5.98        7.91        9.39        11.37       13.51       15.64       17.16       20.14       26.93       25.89       1577.79     
VALUE_RANGE         11.66       6.18        7.70        9.25        11.15       13.57       15.84       17.38       22.07       29.35       29.35       1611.35     
AGG_RANGE           8.70        5.37        6.17        6.70        8.13        9.90        12.11       13.60       17.48       21.73       21.57       1078.39     
AGG_VALUE           64.62       51.19       57.72       60.32       63.53       68.29       73.47       75.79       81.59       87.73       87.65       8903.90     
AGG_RANGE_VALUE     11.22       5.65        7.04        8.77        10.83       12.98       15.28       16.22       25.17       30.47       30.46       1616.79     
GROUP_BY            0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
LATEST_POINT        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
RANGE_QUERY_DESC    11.93       6.08        8.10        9.71        11.67       13.83       15.87       17.11       22.41       26.29       25.88       1609.52     
VALUE_RANGE_QUERY_DESC11.93       6.24        8.09        9.61        11.47       13.75       16.15       17.25       21.71       25.50       25.09       1672.69     
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```