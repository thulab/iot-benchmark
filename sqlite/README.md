Benchmark for SQLite
---

# SQLite
1. 由于SQLite的特性，测试直接运行benchmark即可，会在运行benchmark的目录下创建对应的数据库文件，即`${DB_NAME}.db`和`identifier.sqlite`文件
2. 由于实现原因，同时对文件(数据库)只能有一个Client写入，所以Client_NUMBER必须为**1**

# 实例配置文件
[配置文件](config.properties)

# 实例测试结果
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
OUT_OF_ORDER_MODE: 1
OUT_OF_ORDER_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.03 second
Test elapsed time (not include schema creation): 34.90 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s)
INGESTION           100                 6000                0                   0                   171.90              
PRECISE_POINT       86                  81                  0                   0                   2.32                
TIME_RANGE          87                  4269                0                   0                   122.31              
VALUE_RANGE         95                  4579                0                   0                   131.19              
AGG_RANGE           98                  294                 0                   0                   8.42                
AGG_VALUE           88                  176                 0                   0                   5.04                
AGG_RANGE_VALUE     89                  178                 0                   0                   5.10                
GROUP_BY            92                  1166                0                   0                   33.41               
LATEST_POINT        88                  87                  0                   0                   2.49                
RANGE_QUERY__DESC   87                  4004                0                   0                   114.72              
VALUE_RANGE_QUERY__DESC90                  4033                0                   0                   115.55
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           340.52      194.74      199.71      206.69      218.76      315.36      517.66      688.92      1480.50     2482.81     2436.83     34052.49    
PRECISE_POINT       0.52        0.18        0.19        0.22        0.28        0.74        1.11        1.40        2.72        2.27        2.22        44.61       
TIME_RANGE          0.56        0.21        0.23        0.27        0.38        0.89        1.11        1.36        2.13        1.91        1.89        48.71       
VALUE_RANGE         0.52        0.17        0.20        0.23        0.33        0.76        1.00        1.27        6.04        4.03        3.81        49.53       
AGG_RANGE           0.46        0.20        0.21        0.23        0.28        0.69        0.92        1.22        2.06        1.80        1.77        45.52       
AGG_VALUE           0.83        0.25        0.30        0.35        0.42        1.23        1.88        2.05        2.50        2.47        2.47        73.07       
AGG_RANGE_VALUE     0.42        0.16        0.17        0.21        0.24        0.47        1.06        1.25        1.60        1.60        1.60        37.74       
GROUP_BY            0.56        0.19        0.21        0.25        0.34        0.82        1.02        1.19        2.03        1.85        1.83        51.25       
LATEST_POINT        1.34        0.30        0.35        0.42        0.64        1.47        2.37        4.00        20.48       15.34       14.77       117.83      
RANGE_QUERY__DESC   0.79        0.24        0.25        0.28        0.45        1.05        1.85        2.17        4.71        4.08        4.01        69.04       
VALUE_RANGE_QUERY__DESC0.56        0.19        0.20        0.25        0.34        0.79        1.09        1.35        2.72        2.36        2.32        50.53
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```
