# Benchmark for PIArchive
## The Environment of Test
You need to firstly install the PI JDBC on the test machine, if you want to start this test. Besides, you need to add the jar file of PI JDBC to `Libraries` of the project. The path of jar file is `%PIHOME%\JDBC\PIJDBCDriver.jar`.  
The module of PI will not compile in default. Therefore, you need to cancel the annotation of PI module in `pom.xml`, if you want to compile the module.
```xml
<modules>
        <module>core</module>
        <module>influxdb</module>
        <module>influxdb-2.0</module>
        <module>kairosdb</module>
        <module>opentsdb</module>
        <module>taosdb</module>
        <module>questdb</module>
        <module>timescaledb</module>
        <module>victoriametrics</module>
        <module>iotdb-0.12</module>
        <module>iotdb-0.11</module>
        <module>iotdb-0.10</module>
        <module>iotdb-0.09</module>
        <module>mssqlserver</module>
        <module>sqlite</module>
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
DB_SWITCH: PIArchive
OPERATION_PROPORTION: 0:1:1:1:1:1:1:0:0:1:1
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:1
IS_CLIENT_BIND: true
CLIENT_NUMBER: 5
GROUP_NUMBER: 20
DEVICE_NUMBER: 1
SENSOR_NUMBER: 100
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
Test elapsed time (not include schema creation): 50.85 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           0                   0                   0                   0                   0.00                
PRECISE_POINT       650                 649                 12                  0                   12.76               
TIME_RANGE          607                 30957               0                   0                   608.83              
VALUE_RANGE         645                 32844               6                   0                   645.94              
AGG_RANGE           583                 583                 4                   0                   11.47               
AGG_VALUE           617                 617                 8                   0                   12.13               
AGG_RANGE_VALUE     659                 659                 8                   0                   12.96               
GROUP_BY            0                   0                   0                   0                   0.00                
LATEST_POINT        0                   0                   0                   0                   0.00                
RANGE_QUERY_DESC    586                 29886               6                   0                   587.77              
VALUE_RANGE_QUERY_DESC625                 31875               12                  0                   626.88              
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
PRECISE_POINT       45.07       8.84        16.38       17.90       22.89       69.01       74.35       118.55      124.41      185.87      178.82      6285.25     
TIME_RANGE          44.56       9.25        16.64       18.19       23.20       69.05       74.66       118.67      125.73      173.11      171.93      6385.11     
VALUE_RANGE         44.43       9.33        16.68       18.30       23.74       69.30       75.37       117.74      123.44      126.70      126.33      6799.28     
AGG_RANGE           47.40       8.87        16.54       17.94       24.02       69.90       75.00       118.43      161.00      179.54      177.87      6425.04     
AGG_VALUE           91.27       58.03       65.54       67.40       72.10       118.18      124.00      164.54      172.51      259.20      228.01      12479.66    
AGG_RANGE_VALUE     45.56       9.04        16.80       19.57       25.97       71.48       78.50       119.68      128.76      177.12      174.38      6677.10     
GROUP_BY            0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
LATEST_POINT        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
RANGE_QUERY_DESC    42.80       9.44        16.62       17.99       23.11       69.16       74.80       116.79      124.89      183.14      177.82      5435.38     
VALUE_RANGE_QUERY_DESC46.23       9.34        16.71       18.17       23.64       70.09       77.92       120.08      125.73      168.99      168.68      6003.53     
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

Process finished with exit code 0
```