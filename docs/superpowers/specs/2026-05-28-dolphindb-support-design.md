# DolphinDB 支持设计文档

- **日期**：2026-05-28
- **作者**：JackieTien97 与 Claude（SpriCoder 模式）
- **状态**：已设计，待 review
- **目标分支**：`dolphindb`
- **关联任务**：在 iot-benchmark 中支持 DolphinDB，采用其推荐的最优 Java 读写方式，本地 Docker 安装 DolphinDB，跑通端到端测试。

---

## 1. 背景与目标

iot-benchmark 已支持 IoTDB、InfluxDB、TimescaleDB、TDengine、QuestDB 等 15+ 时序/实时数据库。需要新增 DolphinDB 适配器，达到与现有模块同等水准：

- 可独立打包为发布包（`dolphindb-3.0/target/...`）
- 支持 `IDatabase` 接口的全部 13 个操作（Write + Q1~Q12）
- 采用 DolphinDB 官方推荐的最优写入路径
- 通过 mac 本地 Docker 部署的 DolphinDB 完成 e2e 验证

## 2. 关键设计决策

| 决策点 | 选择 | 理由 |
|---|---|---|
| 本地部署 | Docker（`dolphindb/dolphindb:latest`） | macOS 没有官方原生 server；Docker 启停干净，社区版镜像免授权 |
| Java 写入 API | `MultithreadedTableWriter`（MTW），每客户端 1 个，`threadCount=1` | 官方 IoT 高吞吐首选；写线程总数 = `DATA_CLIENT_NUMBER`，与其它适配器并发语义一致 |
| Java 查询 API | JDBC（`jdbc:dolphindb://host:port`） | 与现有 13 个查询的 ResultSet 流程兼容，便于复用 `executeQueryAndCount` 模板 |
| 模块命名 | `dolphindb-3.0` | 对应 DolphinDB 3.x（Java API jar 版本 `3.00.0.2`），与 `iotdb-2.0`/`tdengine-3.0` 风格一致 |
| 表布局 | 单宽表 `device_data`（列：`ts, deviceId, s_0..s_n`） | 贴合 DolphinDB IoT 官方示例；表数量可控，不随 device 线性膨胀 |
| 分区方案 | 复合分区：一级 `RANGE(ts)` 按 7 天、二级 `HASH([SYMBOL, 1000])` on `deviceId` | RANGE+HASH 是 DolphinDB IoT 官方 best practice；7 天可调；1000 桶对齐官方 IoT demo 粒度 |
| MTW batchSize | 动态等于 `BATCH_SIZE_PER_WRITE × DEVICE_NUM_PER_WRITE`，throttle = 0.01s | 一个 IBatch 恰好凑满一次发送；同步延迟测量准确 |
| MTW 同步策略 | `insertOneBatch` 末尾轮询 `getStatus().unsentRows == 0`（非 `waitForThreadCompletion`） | 后者会终结 MTW 实例；轮询可在整个 client 线程生命周期复用 MTW |
| IS_DELETE_DATA | `dropDatabase('dfs://<DB_NAME>')` | 与 TDengine 适配器一致，最彻底 |
| 数据类型支持 | 全 11 种（BOOLEAN/INT32/INT64/FLOAT/DOUBLE/TEXT/STRING/BLOB/TIMESTAMP/DATE/OBJECT） | DolphinDB 原生覆盖；core 加 DolphinDB 例外分支允许 STRING/BLOB/TIMESTAMP/DATE/OBJECT |

## 3. Section 1 — 模块注册与 core 改动清单

**总计 19 处改动**。

### 3.1 core 模块（8 处）

1. **`core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBType.java`**
   - 新增枚举值 `DolphinDB("DolphinDB")`

2. **`core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBVersion.java`**
   - 新增枚举值 `DolphinDB_3("3")`

3. **`core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBSwitch.java`**
   - 新增 `DB_DOLPHINDB_3(DBType.DolphinDB, DBVersion.DolphinDB_3, null)`

4. **`core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java`**
   - `getDatabase()` switch 加 `case DB_DOLPHINDB_3: dbClass = Constants.DOLPHINDB3_CLASS; break;`

5. **`core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/Constants.java`**
   - 新增常量 `public static final String DOLPHINDB3_CLASS = "cn.edu.tsinghua.iot.benchmark.dolphindb3.DolphinDB";`

6. **`core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/Config.java`**
   - 新增字段 `private int DOLPHINDB_PARTITION_DAYS = 7;` + getter/setter
   - 新增字段 `private int DOLPHINDB_DEVICE_HASH_BUCKETS = 1000;` + getter/setter

7. **`core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/ConfigDescriptor.java`**
   - `loadProps()` 加 2 段（紧跟 `CNOSDB_SHARD_NUMBER` 之后）：
     ```java
     config.setDOLPHINDB_PARTITION_DAYS(
         Integer.parseInt(properties.getProperty(
             "DOLPHINDB_PARTITION_DAYS", config.getDOLPHINDB_PARTITION_DAYS() + "")));
     config.setDOLPHINDB_DEVICE_HASH_BUCKETS(
         Integer.parseInt(properties.getProperty(
             "DOLPHINDB_DEVICE_HASH_BUCKETS", config.getDOLPHINDB_DEVICE_HASH_BUCKETS() + "")));
     ```
   - `checkInsertDataTypeProportion()` 加 DolphinDB 例外：
     ```java
     if (dbType != DBType.IoTDB
         && dbType != DBType.DoubleIoTDB
         && dbType != DBType.DolphinDB) {
       for (int i = config.getTypeNumber() - 4; i < splits.length; i++) { ... }
     }
     ```

8. **`configuration/conf/config.properties`**
   - 在"被测系统为 Influxdb 2.x 时扩展参数"节后追加：
     ```properties
     ############## 被测系统为 DolphinDB 时扩展参数 ########
     # DFS 一级 RANGE(ts) 分区粒度（按天数），默认 7 天一个分区
     # DOLPHINDB_PARTITION_DAYS=7

     # DFS 二级 HASH(deviceId) 分区桶数，默认 1000
     # （对齐 DolphinDB 官方 IoT 示例 dolphindb/tutorials_cn/iot_examples.md）
     # DOLPHINDB_DEVICE_HASH_BUCKETS=1000
     ```

### 3.2 新模块文件（5 个新文件）

9. **根 `pom.xml`** — `<modules>` 新增 `<module>dolphindb-3.0</module>`

10. **`dolphindb-3.0/pom.xml`** — 仿照 `questdb/pom.xml`，依赖：
    - `cn.edu.tsinghua:core:${project.version}`
    - `com.dolphindb:dolphindb-javaapi:3.00.0.2`
    - SLF4J 全套（jcl/jul/api/log4j12）
    - `maven-assembly-plugin` 配置 `<finalName>iot-benchmark-dolphindb-3.0</finalName>`

11. **`dolphindb-3.0/src/assembly/assembly.xml`** — 仿照 `questdb/src/assembly/assembly.xml`，`<id>DolphinDB-3.0</id>`，包含 `bin/`、`conf/`、`lib/`、`benchmark.sh`、`benchmark.bat`、`rep-benchmark.sh`、`cli-benchmark.sh`、`routine`、`LICENSE`

12. **`dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java`** — `IDatabase` 实现（详见 Section 4 & 5）

13. **`dolphindb-3.0/src/main/resources/log4j.properties`** — 复制自 `questdb/src/main/resources/log4j.properties`

### 3.3 文档同步（6 个文档文件）

14. **`README.md`** 改 2 处：
    - 第 2 节 "Supported database types" 表格加一行：`| DolphinDB | v3.x |`
    - 第 3.3 节 "DB_SWITCH" 对照表加一行：`| DolphinDB | 3.x | dolphindb-3.0 | DolphinDB-3 |`

15. **`README-cn.md`** — 中文版对应位置同步修改

16. **`docs/DifferentTestDatabase.md`** — 追加 DolphinDB 节，含 Docker 启动命令、关键配置示例、写入/查询架构说明

17. **`docs/DifferentTestDatabase-cn.md`** — 中文版对应修改

18. **`docs/OperationSupportMatrix.md`** — 矩阵加一行 `**DolphinDB-3**`，按 e2e 测试结果填 ✅/❌

19. **`docs/OperationSupportMatrix-cn.md`** — 中文版对应修改

## 4. Section 2 — DolphinDB.java 写入路径

### 4.1 类骨架

```java
package cn.edu.tsinghua.iot.benchmark.dolphindb3;

public class DolphinDB implements IDatabase {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(DolphinDB.class);

  // 全局共享：schema 仅创建一次
  private static final AtomicBoolean schemaInited = new AtomicBoolean(false);
  private static final AtomicBoolean cleanupDone = new AtomicBoolean(false);
  private static final CyclicBarrier schemaBarrier =
      new CyclicBarrier(config.getSCHEMA_CLIENT_NUMBER());

  // per-instance
  private final DBConfig dbConfig;
  private final String dbPath;        // "dfs://" + DB_NAME
  private static final String TABLE_NAME = "device_data";

  private Connection jdbcConn;            // 查询 + DDL
  private MultithreadedTableWriter mtw;   // 写入

  public DolphinDB(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
    this.dbPath = "dfs://" + dbConfig.getDB_NAME();
  }
}
```

### 4.2 `init()`

只做 JDBC 连接；MTW 延迟到首次 `insertOneBatch` 才创建。这样：

- Schema client 不会因为 MTW 在 schema 还没建好时连接失败
- Data client 在 schema 完成后才被 BaseMode 调度起来，那时 schema 已存在，第一次 `insertOneBatch` 里建 MTW 必成功

```java
public void init() throws TsdbException {
  try {
    String url = String.format(
        "jdbc:dolphindb://%s:%s?user=%s&password=%s",
        dbConfig.getHOST().get(0), dbConfig.getPORT().get(0),
        dbConfig.getUSERNAME(), dbConfig.getPASSWORD());
    jdbcConn = DriverManager.getConnection(url);
  } catch (SQLException e) {
    throw new TsdbException("Failed to connect DolphinDB", e);
  }
}

private synchronized void ensureMtw() throws Exception {
  if (mtw != null) return;
  int batchSize = config.getBATCH_SIZE_PER_WRITE() * config.getDEVICE_NUM_PER_WRITE();
  mtw = new MultithreadedTableWriter(
      dbConfig.getHOST().get(0),
      Integer.parseInt(dbConfig.getPORT().get(0)),
      dbConfig.getUSERNAME(), dbConfig.getPASSWORD(),
      dbPath, TABLE_NAME,
      false, false, null,
      batchSize, 0.01f, 1, "",
      null, MultithreadedTableWriter.Mode.M_Append, null);
}
```

### 4.3 `cleanup()`

- 通过 `cleanupDone` CAS 守门，仅首个进入的线程执行：
  ```sql
  if(existsDatabase("<dbPath>")) { dropDatabase("<dbPath>") }
  ```

### 4.4 `registerSchema(schemaList)`

- 通过 `schemaInited` CAS 守门，仅首个进入的线程执行建库 + 建表
- 其它 schema clients 在 `schemaBarrier.await()` 处等待，确保所有 schema client 在 schema 完成后再返回（参考 TDengine 适配器 `superTableBarrier` 用法）
- 计算 RANGE 边界：
  - `startMs = TimeUtils.convertDateStrToTimestamp(config.getSTART_TIME())`
  - `durationMs = config.getLOOP() * config.getBATCH_SIZE_PER_WRITE() * config.getPOINT_STEP()`
  - `endMs = startMs + durationMs`
  - 按 `DOLPHINDB_PARTITION_DAYS` 切片，生成 `[start, start+7d, start+14d, ..., end]`
- 拼接 DolphinDB 脚本（通过 JDBC `Statement.execute`）：
  ```dolphindb
  rangeBoundaries = [timestamp(<b0>L), timestamp(<b1>L), ..., timestamp(<bN>L)]
  db1 = database("", RANGE, rangeBoundaries)
  db2 = database("", HASH, [SYMBOL, <DOLPHINDB_DEVICE_HASH_BUCKETS>])
  db  = database("<dbPath>", COMPO, [db1, db2])
  schema = table(
      1:0,
      `ts`deviceId`s_0`s_1`...,
      [TIMESTAMP, SYMBOL, <type_0>, <type_1>, ...]
  )
  db.createPartitionedTable(schema, "<TABLE_NAME>", `ts`deviceId)
  ```
- 所有 SCHEMA_CLIENT 在 `schemaBarrier.await()` 处会合，确保后续 init 看到 schema

### 4.5 `insertOneBatch(IBatch batch)`

```java
public Status insertOneBatch(IBatch batch) {
  DeviceSchema device = batch.getDeviceSchema();
  String deviceId = device.getDevice();
  List<Sensor> sensors = device.getSensors();
  try {
    ensureMtw();   // 延迟初始化，确保 schema 已建好
    for (Record record : batch.getRecords()) {
      Object[] row = new Object[2 + sensors.size()];
      row[0] = new java.sql.Timestamp(record.getTimestamp());  // 或 LocalDateTime
      row[1] = deviceId;
      List<Object> vals = record.getRecordDataValue();
      for (int i = 0; i < sensors.size(); i++) {
        row[i + 2] = convertValue(vals.get(i), sensors.get(i).getSensorType());
      }
      ErrorCodeInfo ret = mtw.insert(row);
      if (ret.errorCode != null && !ret.errorCode.isEmpty()) {
        return new Status(false, 0,
            new SQLException(ret.errorInfo), ret.errorInfo);
      }
    }
    // 等待入队数据全部 flush 到 server（不调 waitForThreadCompletion）
    awaitDrain();
    MultithreadedTableWriter.Status st = mtw.getStatus();
    if (st.errorInfo != null && !st.errorInfo.isEmpty()) {
      return new Status(false, 0, new SQLException(st.errorInfo), st.errorInfo);
    }
    return new Status(true);
  } catch (Exception e) {
    LOGGER.error("Failed to insert batch into DolphinDB", e);
    return new Status(false, 0, e, e.toString());
  }
}

private void awaitDrain() throws InterruptedException {
  while (true) {
    MultithreadedTableWriter.Status st = mtw.getStatus();
    if (st.unsentRows == 0) return;
    if (st.errorInfo != null && !st.errorInfo.isEmpty()) return;
    Thread.sleep(1);  // 1 ms 退让，避免空转抢 CPU
  }
}
```

### 4.6 `convertValue(Object, SensorType)` 与 `typeMap`

```java
private static Object convertValue(Object v, SensorType type) {
  switch (type) {
    case BOOLEAN:   return (Boolean) v;
    case INT32:     return (Integer) v;
    case INT64:     return (Long) v;
    case FLOAT:     return (Float) v;
    case DOUBLE:    return (Double) v;
    case TEXT:
    case STRING:    return (String) v;
    case BLOB:
    case OBJECT:    return v instanceof byte[] ? v : String.valueOf(v).getBytes();
    case TIMESTAMP: return new java.sql.Timestamp((Long) v);
    case DATE:      return java.sql.Date.valueOf(String.valueOf(v));
    default:        return String.valueOf(v);
  }
}

@Override
public String typeMap(SensorType iotdbSensorType) {
  switch (iotdbSensorType) {
    case BOOLEAN:   return "BOOL";
    case INT32:     return "INT";
    case INT64:     return "LONG";
    case FLOAT:     return "FLOAT";
    case DOUBLE:    return "DOUBLE";
    case TEXT:
    case STRING:    return "STRING";
    case BLOB:
    case OBJECT:    return "BLOB";
    case TIMESTAMP: return "TIMESTAMP";
    case DATE:      return "DATE";
    default:        return "STRING";
  }
}
```

### 4.7 `close()`

- 若 `mtw != null`（即该 client 写过数据）：`mtw.waitForThreadCompletion()` 收尾，把残余队列 flush 到 server
- 若 `jdbcConn != null`：`jdbcConn.close()`
- 两者各自 try-catch，单边失败不影响另一边关闭

## 5. Section 3 — DolphinDB.java 查询路径

### 5.1 工具方法

```java
private String tableRef() {
  return "loadTable(\"" + dbPath + "\", \"" + TABLE_NAME + "\")";
}

private String tsLiteral(long epochMs) {
  return "timestamp(" + epochMs + "l)";
}

private String deviceInList(List<DeviceSchema> devs) {
  return devs.stream()
      .map(d -> "'" + d.getDevice() + "'")
      .collect(Collectors.joining(", ", "(", ")"));
}

private String sensorColumns(List<Sensor> sensors) {
  return sensors.stream().map(Sensor::getName).collect(Collectors.joining(", "));
}

private Status executeQueryAndCount(String sql) {
  try (Statement st = jdbcConn.createStatement();
       ResultSet rs = st.executeQuery(sql)) {
    int rows = 0;
    while (rs.next()) rows++;
    return new Status(true,
        (long) rows * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM());
  } catch (SQLException e) {
    LOGGER.error("DolphinDB query failed: {}", sql, e);
    return new Status(false, 0, e, e.toString());
  }
}
```

### 5.2 13 个操作 SQL 模板

| Op | DolphinDB SQL |
|---|---|
| Write | MTW 路径（见 Section 2） |
| **Q1** Precise | `SELECT s_0,s_1 FROM loadTable(...) WHERE ts = timestamp(<ts>L) AND deviceId IN ('d_0')` |
| **Q2** Range | `SELECT s_0 FROM loadTable(...) WHERE ts >= timestamp(<s>L) AND ts <= timestamp(<e>L) AND deviceId IN ('d_0')` |
| **Q3** ValueRange | Q2 + `AND s_0 > <thr>` |
| **Q4** AggRange | `SELECT max(s_0) FROM loadTable(...) WHERE ts >= <s> AND ts <= <e> AND deviceId IN (...)` |
| **Q5** AggValue | `SELECT max(s_0) FROM loadTable(...) WHERE s_0 > <thr> AND deviceId IN (...)` |
| **Q6** AggRangeValue | Q4 + value filter |
| **Q7** GroupBy | `SELECT max(s_0) FROM loadTable(...) WHERE ... GROUP BY bar(ts, <granMs>L)` |
| **Q8** Latest | `SELECT last(s_0) FROM loadTable(...) WHERE deviceId = 'd_0'` |
| **Q9** Range DESC | Q2 + `ORDER BY ts DESC` |
| **Q10** ValueRange DESC | Q3 + `ORDER BY ts DESC` |
| **Q11** GroupBy DESC | `SELECT bar(ts, <granMs>L) AS tb, max(s_0) FROM ... GROUP BY tb ORDER BY tb DESC` |
| **Q12** SetOp | 两次范围查询用 `UNION` / `INTERSECT` / `EXCEPT` 拼接 |

### 5.3 DolphinDB SQL 语法要点

1. **时间字面量**：用 `timestamp(<epochMs>L)` 函数，避开文本日期格式化的时区坑。`L` 后缀确保 LONG 解析。
2. **设备字面量**：`deviceId` 列是 SYMBOL，SQL 里用 `'d_0'` 字符串字面量，DolphinDB 隐式转换。
3. **`loadTable(...)`**：在 FROM 子句直接调用，无需 `share`。
4. **`bar(col, interval)`**：时间桶函数，参数 2 是 LONG（ms）。
5. **`last(col)`**：聚合函数，TSDB 引擎下 O(1) 取末。
6. **Q11**：DolphinDB 不支持 `ORDER BY bar(...) DESC` 表达式排序，需别名包装。
7. **`UNION`/`INTERSECT`/`EXCEPT`**：原生支持，子查询用括号包裹。

## 6. Section 4 — Docker 安装 + e2e 测试流程

### 6.1 镜像准备

```bash
docker pull dolphindb/dolphindb:latest
# Apple Silicon 显式指定 ARM64：
docker pull --platform linux/arm64 dolphindb/dolphindb:latest
```

### 6.2 启动单节点

```bash
docker run -itd --name ddb \
  -p 8848:8848 \
  --ulimit nofile=65536:65536 \
  dolphindb/dolphindb:latest \
  sh

# 验证：访问 http://127.0.0.1:8848 可见 DolphinDB Web GUI
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8848
```

### 6.3 Benchmark 配置

修改 `dolphindb-3.0/target/iot-benchmark-dolphindb-3.0/iot-benchmark-dolphindb-3.0/conf/config.properties`：

```properties
DB_SWITCH=DolphinDB-3
HOST=127.0.0.1
PORT=8848
USERNAME=admin
PASSWORD=123456
DB_NAME=benchmark
DOLPHINDB_PARTITION_DAYS=7
DOLPHINDB_DEVICE_HASH_BUCKETS=1000

# Smoke test 参数
LOOP=100
DEVICE_NUMBER=10
SENSOR_NUMBER=5
BATCH_SIZE_PER_WRITE=100
SCHEMA_CLIENT_NUMBER=2
DATA_CLIENT_NUMBER=2
IS_DELETE_DATA=true
CREATE_SCHEMA=true
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1:0:0:0:0:0
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0:0:0
```

### 6.4 构建与运行

```bash
mvn clean package -pl dolphindb-3.0 -am -Dmaven.test.skip=true
cd dolphindb-3.0/target/iot-benchmark-dolphindb-3.0/iot-benchmark-dolphindb-3.0
./benchmark.sh
```

### 6.5 验证判据

1. 终端无 ERROR；最终输出 `All dataClients finished`
2. `Create schema cost X second`
3. Result Matrix 的 `INGESTION okOperation > 0` 且 `failOperation = 0`
4. `throughput(point/s)` 数量级与 QuestDB/TDengine 相近
5. 在 DolphinDB Web GUI 执行：
   ```dolphindb
   pt = loadTable("dfs://benchmark", "device_data")
   select count(*) from pt
   select count(distinct deviceId) from pt
   ```
   分别应等于 `LOOP × BATCH_SIZE × DEVICE_NUMBER / DATA_CLIENT_NUMBER` 和 `DEVICE_NUMBER`
6. 切到 `OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1:1:0`，验证 Q1~Q11 都 `okOperation > 0` 且 `failOperation = 0`
7. 切到 `OPERATION_PROPORTION=0:0:0:0:0:0:0:0:0:0:0:0:1`，验证 Q12（UNION）也通

### 6.6 排查清单

| 现象 | 排查方向 |
|---|---|
| MTW stuck | DolphinDB 端口/账户/dfs path 错；docker 端口未映射 |
| schema 失败 `database already exists` | `IS_DELETE_DATA=false` 但库残留；改 true 重跑 |
| status.errorInfo 非空 | 列类型与 schema 不符；BLOB 列收到 String —— 检查 typeMap |
| 查询返 0 行但应有数据 | timestamp() 字面量精度错位；START_TIME 时区差异 |
| ARM Mac 拉镜像卡 | `--platform linux/arm64` 强制 |

## 7. 风险与未决项

| 项 | 描述 | 缓解措施 |
|---|---|---|
| DolphinDB JDBC `?` 占位符 | JDBC 对参数化查询的支持可能不全 | 全部用 SQL 拼接（已规避） |
| Q11 别名兼容性 | `bar()` 别名 + `ORDER BY` 在所有 DolphinDB 版本未必兼容 | e2e 测试时若失败，回落到客户端排序 |
| OBJECT/BLOB 大数据 | DolphinDB BLOB 列写入大对象 vs 性能 | benchmark 默认不开 OBJECT，需要时单独验证 |
| MTW threadCount=1 vs 多 | 我们选每客户端 1 个 MTW；DolphinDB 内置并发被关闭 | 若实测吞吐显著低于其他列存 DB，再评估改共享 MTW |
| ARM64 镜像可用性 | DolphinDB 官方 ARM64 镜像 tag 名 | 拉取时若失败，回落到 x86 emulation |

## 8. 后续步骤

1. 用 writing-plans 技能把本 spec 拆成可执行的实施计划
2. 按计划落地 19 处改动
3. 跑 e2e，按 §6.5 判据验证
4. 根据实测填回 `docs/OperationSupportMatrix.md` 的 ✅/❌

