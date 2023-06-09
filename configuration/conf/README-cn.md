# config.propertities部分配置项说明

## DEVICE_NUM_PER_WRITE

以下简称为DNW。

### 含义

单次写入所包含的设备数量。

### 与其它参数的搭配

DNW=1时，搭配效果应与加入跨设备Batch之前完全一致，**此处分析DNW＞1的情况**

- 与DB_SWITCH：
  - 数据库版本：目前支持IoTDB-013、IoTDB-110
  - insert方式：SESSION_BY_RECORDS模式允许DNW>1，SESSION_BY_RECORD、SESSION_BY_TABLET模式只允许DNW=1，参数不匹配则报错
- 与BATCH_SIZE_PER_WRITE：
  - BATCH_SIZE_PER_WRITE含义为单次写入的record总数，而DNW则决定这些record会被平均分配到多少个设备上
  - BATCH_SIZE_PER_WRITE必须能够被DNW整除
- 与IS_CLIENT_BIND：支持true与false
- 与IS_SENSOR_TS_ALIGNMENT：暂不支持此配置项为false，但是不会报错，而是按true处理

