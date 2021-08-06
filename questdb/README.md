QuestDB 测试实验报告
---
1. 请注意：QuestDB的CLIENT_NUMBER需要在启动时完成配置(与pg.net.active.connection.limit相关)！
2. 请注意：Sensor数量收到QuestDB的影响，不能过多，推荐控制在100及以内

# 测试环境（Docker）
1. 拉取镜像：`docker pull questdb/questdb`
2. 启动镜像：`docker run --rm -p 9000:9000  -p 9009:9009  -p 8812:8812  -p 9003:9003  -e QDB_CAIRO_MAX_UNCOMMITTED_ROWS=100000  -e QDB_CAIRO_COMMIT_LAG=20000 -e QDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL=1 -e QBD_SHARED_WORKER_COUNT=10 -e QDB_PG_WORKER_COUNT=0 --name=questdb questdb/questdb`
3. 服务器部署补充说明，请在启动服务器前，执行如下命令设置，更多参考：https://questdb.io/docs/reference/configuration#postgres-wire-protocol

```
export QDB_CAIRO_MAX_UNCOMMITTED_ROWS=100000
export QDB_CAIRO_COMMIT_LAG=20000
export QDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL=1
export QBD_SHARED_WORKER_COUNT=10
export QDB_PG_WORKER_COUNT=0
```

```conf
cairo.max.uncommitted.rows=100000
cairo.commit.lag=20000
line.tcp.maintenance.job.interval=1
shared.worker.count=10
pg.worker.count=0
```

# Config文件参数
[配置文件](config.properties)

# 长写测试结果
```
----------------------Main Configurations----------------------
DB_SWITCH: QuestDB
OPERATION_PROPORTION: 1:1:1:1:1:1:1:1:1:1:1
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
Create schema cost 0.10 second
Test elapsed time (not include schema creation): 30.96 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           478                 47800               0                   0                   1543.92             
PRECISE_POINT       462                 1325                0                   0                   42.80               
TIME_RANGE          438                 59230               0                   0                   1913.11             
VALUE_RANGE         483                 66475               0                   0                   2147.12             
AGG_RANGE           425                 2125                0                   0                   68.64               
AGG_VALUE           460                 2300                0                   0                   74.29               
AGG_RANGE_VALUE     431                 2155                0                   0                   69.61               
GROUP_BY            473                 2365                0                   0                   76.39               
LATEST_POINT        459                 2295                0                   0                   74.13               
RANGE_QUERY__DESC   433                 61645               0                   0                   1991.11             
VALUE_RANGE_QUERY__DESC458                 63720               0                   0                   2058.13             
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           35.37       9.64        24.77       28.90       32.60       40.47       50.01       55.17       66.74       129.00      111.17      4403.75     
PRECISE_POINT       2.51        0.27        0.46        0.75        2.32        3.51        4.73        5.63        8.11        10.82       10.28       358.54      
TIME_RANGE          2.52        0.26        0.45        0.66        2.41        3.63        4.59        5.70        8.37        9.95        9.85        340.73      
VALUE_RANGE         2.74        0.32        0.51        0.94        2.57        3.83        5.16        6.16        8.63        13.48       12.64       447.13      
AGG_RANGE           2.42        0.27        0.45        0.76        2.28        3.59        4.59        5.15        7.04        9.53        8.94        276.42      
AGG_VALUE           2.47        0.21        0.37        0.72        2.30        3.47        4.44        5.81        8.50        28.66       22.99       365.44      
AGG_RANGE_VALUE     2.59        0.30        0.51        0.81        2.47        3.75        4.53        5.98        7.91        8.86        8.81        365.65      
GROUP_BY            2.61        0.28        0.50        1.14        2.47        3.76        4.63        5.94        7.66        12.09       11.43       358.28      
LATEST_POINT        149.22      0.20        0.42        0.73        2.29        3.59        4.84        6.14        16.13       27781.60    25139.06    25303.78    
RANGE_QUERY__DESC   4.79        0.60        2.15        3.38        4.47        5.90        7.53        8.72        11.12       41.56       33.35       507.17      
VALUE_RANGE_QUERY__DESC4.99        0.65        2.39        3.62        4.69        6.09        7.57        9.35        11.65       13.86       13.41       567.92      
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```