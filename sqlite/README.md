Benchmark for SQLite
---

# SQLite
1. Due to the characteristics of SQLite, the test can run the benchmark directly, and the corresponding database files will be created in the directory where the benchmark is run, namely the `${DB_NAME}.db` and `identifier.sqlite` files
2. Due to implementation reasons, only one Client can write to the file (database) at the same time, so Client_NUMBER must be **1**

# config
[Demo config](config.properties)

# test result
```
----------------------Main Configurations----------------------
DB_SWITCH: SQLite
OPERATION_PROPORTION: 1:1:1:1:1:1:1:1:1:1:1
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:1
IS_CLIENT_BIND: true
CLIENT_NUMBER: 1
GROUP_NUMBER: 20
DEVICE_NUMBER: 1
SENSOR_NUMBER: 6
BATCH_SIZE_PER_WRITE: 10
LOOP: 1000
POINT_STEP: 5000
QUERY_INTERVAL: 250000
IS_OUT_OF_ORDER: false
OUT_OF_ORDER_MODE: 0
OUT_OF_ORDER_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.07 second
Test elapsed time (not include schema creation): 34.76 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           100                 6000                0                   0                   172.62              
PRECISE_POINT       86                  0                   0                   0                   0.00                
TIME_RANGE          87                  4183                0                   0                   120.35              
VALUE_RANGE         95                  4486                0                   0                   129.06              
AGG_RANGE           98                  294                 0                   0                   8.46                
AGG_VALUE           88                  176                 0                   0                   5.06                
AGG_RANGE_VALUE     89                  178                 0                   0                   5.12                
GROUP_BY            92                  1140                0                   0                   32.80               
LATEST_POINT        88                  87                  0                   0                   2.50                
RANGE_QUERY_DESC    87                  3920                0                   0                   112.78              
VALUE_RANGE_QUERY_DESC90                  3948                0                   0                   113.58              
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           338.99      283.08      297.15      324.15      337.24      352.62      357.66      360.86      377.58      629.06      615.16      33898.57    
PRECISE_POINT       0.54        0.29        0.32        0.40        0.50        0.64        0.79        0.82        1.88        1.52        1.48        46.27       
TIME_RANGE          0.64        0.29        0.35        0.46        0.59        0.78        0.93        1.02        1.65        1.45        1.43        55.74       
VALUE_RANGE         0.54        0.29        0.34        0.40        0.51        0.65        0.72        0.90        1.26        1.15        1.14        50.89       
AGG_RANGE           0.53        0.22        0.33        0.42        0.51        0.61        0.72        0.89        1.12        1.08        1.08        51.87       
AGG_VALUE           0.72        0.29        0.46        0.58        0.68        0.79        1.03        1.12        1.71        1.48        1.46        63.22       
AGG_RANGE_VALUE     0.42        0.20        0.25        0.30        0.40        0.52        0.63        0.66        0.95        0.87        0.86        37.80       
GROUP_BY            0.53        0.25        0.33        0.42        0.51        0.63        0.70        0.77        1.07        1.01        1.01        48.55       
LATEST_POINT        0.87        0.40        0.57        0.68        0.79        0.98        1.22        1.29        4.00        2.93        2.81        76.34       
RANGE_QUERY_DESC    0.65        0.30        0.40        0.53        0.63        0.76        0.90        0.98        1.89        1.55        1.51        56.77       
VALUE_RANGE_QUERY_DESC0.52        0.23        0.31        0.38        0.50        0.64        0.72        0.81        0.94        0.92        0.92        46.56       
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```
