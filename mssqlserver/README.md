Microsoft SQL Server
---

# 配置环境
1. MS SQL 2016 SP2 standard version
2. 访问软件：Microsoft SQL Server Management(HSSM)

# 前置条件
1. MS SQL Server必须开放1433端口
2. MS SQL Server必须允许通过TCP/IP访问
3. MS SQL Server中必须已经预先创建和DB_NAME保持一致的Database，使用sqlcmd：`create database ${DB_NAME}`
4. MS SQL Server中必须已经存在test用户，密码为12345678，并且拥有DB_NAME对应数据库的对应权限，创建命令(授予全部权限)如下：

```
USE [master]
GO
CREATE LOGIN [test] WITH PASSWORD=N'12345678' MUST_CHANGE, DEFAULT_DATABASE=[master], CHECK_EXPIRATION=ON, CHECK_POLICY=ON
GO
ALTER SERVER ROLE [bulkadmin] ADD MEMBER [test]
GO
ALTER SERVER ROLE [dbcreator] ADD MEMBER [test]
GO
ALTER SERVER ROLE [diskadmin] ADD MEMBER [test]
GO
ALTER SERVER ROLE [processadmin] ADD MEMBER [test]
GO
ALTER SERVER ROLE [securityadmin] ADD MEMBER [test]
GO
ALTER SERVER ROLE [serveradmin] ADD MEMBER [test]
GO
ALTER SERVER ROLE [setupadmin] ADD MEMBER [test]
GO
ALTER SERVER ROLE [sysadmin] ADD MEMBER [test]
GO
```

5. 在数据库的properties中的Security选项卡中设置server authentication设置为SQL Server and xxx(允许通过用户名和密码访问)

# 样例测试config文件
[样例测试Config文件详情](config.properties)

# 样例测试结果
```
----------------------Main Configurations----------------------
DB_SWITCH: MSSQLSERVER
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
Create schema cost 0.00 second
Test elapsed time (not include schema creation): 20.35 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           478                 47800               0                   0                   2348.97             
PRECISE_POINT       462                 870                 0                   0                   42.75               
TIME_RANGE          438                 37958               0                   0                   1865.32             
VALUE_RANGE         483                 44077               0                   0                   2166.02             
AGG_RANGE           425                 425                 0                   0                   20.89               
AGG_VALUE           460                 460                 0                   0                   22.61               
AGG_RANGE_VALUE     431                 431                 0                   0                   21.18               
GROUP_BY            473                 90                  0                   0                   4.42                
LATEST_POINT        459                 773                 0                   0                   37.99               
RANGE_QUERY__DESC   433                 36849               0                   0                   1810.82             
VALUE_RANGE_QUERY__DESC458                 38707               0                   0                   1902.13             
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           43.19       21.57       31.41       37.18       41.98       47.09       53.85       58.26       75.06       204.86      203.77      4462.05     
PRECISE_POINT       18.18       2.09        6.08        12.58       18.61       24.03       27.70       29.62       33.45       91.28       73.51       1797.75     
TIME_RANGE          18.60       1.81        6.75        13.53       19.22       24.27       27.42       29.27       34.15       37.45       37.24       1959.17     
VALUE_RANGE         19.57       2.07        8.95        13.37       19.63       24.86       29.16       31.62       35.40       212.23      156.51      2123.29     
AGG_RANGE           18.90       1.98        7.51        13.24       19.63       24.90       29.11       30.96       33.92       39.33       38.15       1991.18     
AGG_VALUE           19.93       2.16        6.16        13.60       20.45       26.39       31.17       34.14       37.85       48.58       46.07       1969.33     
AGG_RANGE_VALUE     18.63       2.02        7.30        13.07       18.57       24.45       28.23       30.23       34.10       245.30      182.78      1753.60     
GROUP_BY            16.39       5.10        9.53        12.49       15.44       18.81       21.98       23.79       37.48       162.25      142.00      1724.70     
LATEST_POINT        12.13       2.27        4.80        6.63        10.22       14.60       21.68       28.01       43.38       81.18       73.06       1230.08     
RANGE_QUERY__DESC   13.20       3.91        7.49        10.17       12.97       16.08       18.44       20.34       23.38       61.84       50.82       1220.04     
VALUE_RANGE_QUERY__DESC15.94       3.04        8.36        11.20       14.23       17.72       20.31       24.83       73.69       155.70      152.34      1607.59     
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```