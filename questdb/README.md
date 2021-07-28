QuestDB 测试实验报告
---
1. 请注意：QuestDB的CLIENT_NUMBER不能超过10个

# 测试环境（Docker）
1. 拉取镜像：`docker pull questdb/questdb`
2. 启动镜像：`docker run -p 9000:9000  -p 9009:9009  -p 8812:8812  -p 9003:9003  -e QDB_CAIRO_MAX_UNCOMMITTED_ROWS=100000  -e QDB_CAIRO_COMMIT_LAG=20000 -e QDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL=1 questdb/questdb`

# 长写测试结果
```
----------------------Main Configurations----------------------
DB_SWITCH: QuestDB
OPERATION_PROPORTION: 1:0:0:0:0:0:0:0:0:0:0
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
Create schema cost 0.07 second
Test elapsed time (not include schema creation): 32.95 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           5000                500000              0                   0                   15176.25            
PRECISE_POINT       0                   0                   0                   0                   0.00                
TIME_RANGE          0                   0                   0                   0                   0.00                
VALUE_RANGE         0                   0                   0                   0                   0.00                
AGG_RANGE           0                   0                   0                   0                   0.00                
AGG_VALUE           0                   0                   0                   0                   0.00                
AGG_RANGE_VALUE     0                   0                   0                   0                   0.00                
GROUP_BY            0                   0                   0                   0                   0.00                
LATEST_POINT        0                   0                   0                   0                   0.00                
RANGE_QUERY__DESC   0                   0                   0                   0                   0.00                
VALUE_RANGE_QUERY__DESC0                   0                   0                   0                   0.00                
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           20.83       0.89        2.79        4.56        5.90        8.11        10.30       11.78       15.76       78.97       26853.09    32749.50    
PRECISE_POINT       0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
TIME_RANGE          0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE         0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_RANGE           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_VALUE           0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
AGG_RANGE_VALUE     0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
GROUP_BY            0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
LATEST_POINT        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
RANGE_QUERY__DESC   0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
VALUE_RANGE_QUERY__DESC0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        0.00        
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```