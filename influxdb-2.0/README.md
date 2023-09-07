Benchmark influxdb 2.0
---
This project is using iot-benchmark to test influxdb 2

Notice, default config of influxdb2 is not support too many write in a short time.

# environment
1. influxdb: 2.7.0

# database setup
1. visit http://{ip}:8086/ to set up user
    1. username: admin
    2. password: 12345678
    3. Initial Organization Name: company1
    4. Initial Bucket Name: test
![img.png](https://github.com/thulab/iot-benchmark/assets/38746920/cc6612a5-8a42-4e21-a609-c80e632da1fc)
# config
1. This is [Demo config](config.properties)
2. This is [config.yaml for InfluxDB v2.0](config.yaml), more details: https://docs.influxdata.com/influxdb/v2.7/reference/config-options/
3. [Syntax](https://docs.influxdata.com/influxdb/v2.0/reference/flux/)
4. In config.properties, you should pay attention to 
   1. DB_SWITCH=InfluxDB-2.x 
   2. PORT=8086
   3. USERNAME=admin(same with username in http://{ip}:8086/)
   4. PASSWORD=12345678(same with password in http://{ip}:8086/)
   5. DB_NAME=test(same with Initial Bucket Name in http://{ip}:8086/)
   6. INFLUXDB_ORG=company1(same with Initial Organization Name in http://{ip}:8086/)
   7. TOKEN(can be found at http://{ip}:8086/)
   ![image](https://user-images.githubusercontent.com/34939716/149779954-29d9485d-d750-4313-ab45-2e4aaff9c7e8.png)
   ![image](https://user-images.githubusercontent.com/34939716/149780004-fc430061-5e4a-4ea2-8cbc-730cb6e518e0.png)

# test result
```
----------------------Main Configurations----------------------
DB_SWITCH: InfluxDB-2.0
OPERATION_PROPORTION: 1:1:1:1:1:1:1:1:1:1:1
ENABLE_THRIFT_COMPRESSION: false
INSERT_DATATYPE_PROPORTION: 1:1:1:1:1:1
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
OUT_OF_ORDER_MODE: 0
OUT_OF_ORDER_RATIO: 0.5
---------------------------------------------------------------
main measurements:
Create schema cost 0.06 second
Test elapsed time (not include schema creation): 28.69 second
----------------------------------------------------------Result Matrix----------------------------------------------------------
Operation           okOperation         okPoint             failOperation       failPoint           throughput(point/s) 
INGESTION           478                 47800               0                   0                   1666.08             
PRECISE_POINT       462                 101                 0                   0                   3.52                
TIME_RANGE          438                 16663               0                   0                   580.79              
VALUE_RANGE         483                 18881               0                   0                   658.10              
AGG_RANGE           425                 397                 0                   0                   13.84               
AGG_VALUE           460                 459                 0                   0                   16.00               
AGG_RANGE_VALUE     431                 404                 0                   0                   14.08               
GROUP_BY            473                 441                 0                   0                   15.37               
LATEST_POINT        459                 454                 0                   0                   15.82               
RANGE_QUERY_DESC    433                 16354               0                   0                   570.02              
VALUE_RANGE_QUERY_DESC458                 17586               0                   0                   612.97              
---------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------
Operation           AVG         MIN         P10         P25         MEDIAN      P75         P90         P95         P99         P999        MAX         SLOWEST_THREAD
INGESTION           11.30       4.38        5.27        5.94        6.88        10.05       21.58       31.20       62.82       174.20      171.18      1279.96     
PRECISE_POINT       26.72       15.69       19.64       21.51       24.07       28.81       35.37       42.07       65.09       85.21       83.11       2626.28     
TIME_RANGE          28.57       17.01       20.15       22.77       25.45       30.57       38.03       45.41       64.52       250.85      213.17      2789.76     
VALUE_RANGE         31.42       19.31       23.06       25.50       28.83       34.00       42.35       50.13       63.38       168.34      146.18      3262.28     
AGG_RANGE           27.51       17.30       20.49       22.61       25.39       30.15       37.09       41.61       60.12       71.47       71.26       2570.26     
AGG_VALUE           33.69       20.66       24.91       27.53       30.58       36.00       46.03       51.79       87.04       133.96      122.10      3365.90     
AGG_RANGE_VALUE     31.97       20.10       24.21       26.33       29.11       35.07       43.22       51.18       61.40       88.96       88.72       3051.53     
GROUP_BY            28.94       17.28       21.14       23.44       26.06       31.81       39.20       45.23       67.95       139.30      126.26      2868.69     
LATEST_POINT        29.65       17.79       20.77       22.70       25.89       30.43       39.06       48.71       128.56      151.48      151.48      3118.76     
RANGE_QUERY_DESC    29.66       16.74       22.57       24.67       27.82       31.90       39.19       46.46       56.90       62.98       62.92       2645.49     
VALUE_RANGE_QUERY_DESC33.76       21.71       25.19       27.50       31.09       36.87       44.24       52.07       78.34       119.55      117.49      3330.26     
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```
