## 1. 数据写入的分配与节奏

本文说明 benchmark 在写入时，**设备如何映射到表和 client、每个 client 按什么顺序写、时间戳如何分配**。
理解这套机制对估算数据量、判断压测是否打满、以及排查"某张表没数据"类问题都是必要的。

> 注意：本文描述的是**随机数据生成**路径（`BENCHMARK_WORK_MODE=testWithDefaultPath` 或 `generateDataMode`）。
> `verificationWriteMode` / `verificationQueryMode` 走的是 CSV 回放路径，分配方式不同。

### 1.1. 三层映射

写入涉及三层实体，映射关系在**启动时一次性算定**，运行期不再变化：

```
设备(device)  ──①──▶  表(table)  ──②──▶  数据库(database)  ──③──▶  client(线程)
```

#### ① 设备 → 表

由 `DeviceSchema` 构造函数通过 `MetaUtil.mappingId` 计算，默认 `SG_STRATEGY=mod`：

```
tableId = deviceId % IoTDB_TABLE_NUMBER
```

因此表 `t` 拥有的设备为 `{t, t + M, t + 2M, ...}`，其中 `M = IoTDB_TABLE_NUMBER`。
每张表的设备数恰好是 `DEVICE_NUMBER / IoTDB_TABLE_NUMBER`。

#### ② 设备顺序重排（关键）

`MetaUtil.sortDeviceId()` 会把设备**按表聚簇重排**，得到一个扁平列表 `deviceIds`：

```
[表0的全部设备..., 表1的全部设备..., ..., 表M-1的全部设备...]
```

重排的目的是保证**一个 batch 里的多台设备来自同一张表**（见 `MetaUtil.sortDeviceId` 的注释）。
`GROUP_NUMBER=1` 时，按数据库分桶这一步不会打乱上述顺序。

#### ③ 设备 → client

`MetaUtil.distributeDevices()` 在重排后的 `deviceIds` 上做**连续切片**，依次分给每个 client：

```
每个 client 的基础设备数 = DEVICE_NUMBER / DATA_CLIENT_NUMBER
余数 DEVICE_NUMBER % DATA_CLIENT_NUMBER 台，依次多分给前几个 client
```

因为设备已经按表聚簇，连续切片等价于**按表切片**：只要 `DEVICE_NUMBER / IoTDB_TABLE_NUMBER`
整除于 `DEVICE_NUMBER / DATA_CLIENT_NUMBER`，每个 client 就拿到整数张表，且**任意两个 client 不共享表**。

> `distributeDevices()` 会对 schema client 和 data client 各调用一次（`GenerateMetaDataSchema.createMetaDataSchema`）。
> 当 `SCHEMA_CLIENT_NUMBER == DATA_CLIENT_NUMBER` 时两者拿到相同的设备切片。

### 1.2. 单个 client 的写入节奏

以 `testWithDefaultPath` 为例，`GenerateDataMixClient` 的主循环是：

```java
for (; taskProgress.getLoopIndex() < config.getLOOP(); taskProgress.incrementLoopIndex()) {
    Operation operation = operationController.getNextOperationType();
    if (operation == Operation.INGESTION) {
        ingestionOperation();      // ← 一次调用走完该 client 的【全部设备】
    }
    ...
}
```

而 `ingestionOperation()` 内部是：

```java
for (int i = 0; i < clientDeviceSchemas.size(); i += config.getDEVICE_NUM_PER_WRITE()) {
    IBatch batch = dataWorkLoad.getOneBatch();
    if (checkBatch(batch)) {
        dbWrapper.insertOneBatchWithCheck(batch);
    }
}
insertLoopIndex++;
```

也就是说：

- **一次 `LOOP` 迭代 = 该 client 完整扫一遍它负责的所有设备 = 一个 "pass"**
- 一次 pass 内，设备是**串行**写的：设备 → 设备 → … → 最后一台，然后 `insertLoop++`，回到第一台重新开始
- 落在哪张表上，取决于该设备属于哪张表；因为设备按表聚簇，所以轨迹是 **表 A 的 100 台 → 表 B 的 100 台 → …**

`DEVICE_NUM_PER_WRITE > 1` 时，是每 `DEVICE_NUM_PER_WRITE` 台设备合并成一个 batch 写（一个 tablet 装多台设备），
但遍历顺序不变。

### 1.3. client 之间是并行的，但相位会漂移

`DATA_CLIENT_NUMBER` 个 client 是 **`CyclicBarrier` 对齐起点后各跑各的**：

- 启动瞬间它们确实同时开始
- 但彼此**没有任何阶段同步**，谁也不会等谁
- 各 client 的写入速度受数据分布、服务端负载影响，跑一会儿之后**相位就错开了**

所以「20 个 client 同时在写哪 20 张表」这个问题，答案只在启动那一刻是确定的，
运行期是漂移的；唯一确定的是**每个 client 只写自己那 50 张表**。

### 1.4. 时间戳分配

`SyntheticDataWorkLoad` 里（`IS_CLIENT_BIND=true`，默认）：

```java
long rowOffset = insertLoop * config.getBATCH_SIZE_PER_WRITE();
...
records.add(new Record(getCurrentTimestamp(rowOffset), generateOneRow(...)));
```

注意 `rowOffset` **只依赖 `insertLoop`，不依赖 `deviceIndex`**。这意味着：

- **同一个 pass 内，该 client 的所有设备写的是完全相同的 `BATCH_SIZE_PER_WRITE` 个时间点**
- 第 `p` 个 pass 覆盖 `rowOffset ∈ [p*B, (p+1)*B)`，相邻 pass 首尾相接
- 每个设备的总行数 = `LOOP * BATCH_SIZE_PER_WRITE`

周期频率下（`IS_REGULAR_FREQUENCY=true`，默认），时间戳为
`START_TIMESTAMP * precision + POINT_STEP * rowOffset + POINT_STEP`。

### 1.5. 实例演算

以「1000 张表、每表 100 device、10 列、并发 20」为例：

```
IoTDB_TABLE_NUMBER    = 1000
DEVICE_NUMBER         = 1000 × 100 = 100000      # 表数 × 每表设备数
SENSOR_NUMBER         = 10
DATA_CLIENT_NUMBER    = 20
LOOP                  = 60000
BATCH_SIZE_PER_WRITE  = 1000
DEVICE_NUM_PER_WRITE  = 1                        # 默认值
```

代入上面的规则：

| 量 | 计算 | 结果 |
|---|---|---|
| 每 client 设备数 | 100000 / 20 | 5000 台 |
| 每 client 表数 | 5000 / 100 | **50 张** |
| client 0 负责的表 | — | 表 0..49 |
| client 1 负责的表 | — | 表 50..99 |
| … | … | … |
| client 19 负责的表 | — | 表 950..999 |
| 每设备行数 | 60000 × 1000 | 6000 万行 |
| 每设备点数 | 6000万 × 10 | 6 亿点 |
| **总点数** | 100000 × 10 × 60000 × 1000 | **6 × 10¹³（60 万亿）** |

**不是「20 张表同时写、写完再写其他表」**，而是：

> 20 个 client 各自绑定 50 张表，各自从自己第一张表的设备开始**串行**写；
> 写完自己这 50 张表（= 一次 `LOOP` 迭代），再从头开始下一轮，共 `LOOP` 轮。

### 1.6. 相关配置项

| 配置项 | 含义 | 默认值 |
|---|---|---|
| `DEVICE_NUMBER` | 设备总数 | 6000 |
| `SENSOR_NUMBER` | 每个设备的列（测点）数 | 200 |
| `IoTDB_TABLE_NUMBER` | 表模型下的表数量 | 1 |
| `GROUP_NUMBER` | 数据库数量 | 1 |
| `DATA_CLIENT_NUMBER` | 写入并发线程数 | 20 |
| `SCHEMA_CLIENT_NUMBER` | 建 schema 的线程数 | 20 |
| `LOOP` | 每个 client 的 pass 数 | 100 |
| `BATCH_SIZE_PER_WRITE` | 每个 batch 的行数（每台设备） | 100 |
| `DEVICE_NUM_PER_WRITE` | 一个 batch 合并几台设备 | 1 |
| `SG_STRATEGY` | 设备→表的映射策略（mod/hash/div） | mod |
| `IS_CLIENT_BIND` | 是否把设备静态绑定到 client | true |
| `IS_SENSOR_TS_ALIGNMENT` | 所有列是否共用同一时间戳 | true |
| `POINT_STEP` | 相邻行的时间间隔(ms) | 5000 |

### 1.7. 约束与校验

`ConfigDescriptor.checkDeviceNumPerWrite()` 会在启动时校验以下条件，不满足直接报错退出：

- `DEVICE_NUM_PER_WRITE > 0`
- `DEVICE_NUM_PER_WRITE > 1` 时：
  - `IS_SENSOR_TS_ALIGNMENT` 必须为 `true`
  - 只支持 IoTDB / DolphinDB
  - 每个 client 分到的设备数必须能被它整除
- 表模型下：
  - `DEVICE_NUMBER % IoTDB_TABLE_NUMBER == 0`
  - 每张表的设备数必须能被 `DEVICE_NUM_PER_WRITE` 整除
  - `DATA_CLIENT_NUMBER % GROUP_NUMBER == 0`（保证一个 client 只写给一个库）

`MetaUtil.distributeDevices()` 在表模型下还会额外校验每个 client 的设备不出自多个数据库。

### 1.8. 常见误区

**「加了并发就应该更快填满所有表」**
不会。client 与表是静态绑定的：client 0 永远不会写表 999。想调整每 client 的表数，改
`DATA_CLIENT_NUMBER` 或 `LOOP` 的组合，而不是指望调度。

**「DEVICE_NUM_PER_WRITE 调大能减少表切换」**
可以合并设备、减少 RPC 次数，但**batch 不会跨表**（这正是 `sortDeviceId()` 重排要保证的）。
且内存占用按 `DEVICE_NUM_PER_WRITE × BATCH_SIZE_PER_WRITE` 线性增长。

**「总点数是设备数 × 列数 × LOOP」**
少乘了 `BATCH_SIZE_PER_WRITE`。注意该参数在 `generateDataMode` 之外**同时控制行数和时间跨度**：
它是每个 pass 的行数，也是相邻 pass 的时间跨度。
