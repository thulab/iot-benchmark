# 1. 使用 IoT Benchmark 测试其他数据库

本页是当前仓库中各数据库模块快速指引的索引。

> 注意：
> 1. 下方链接均指向各数据库模块自己的 README 文件。
> 2. 部分模块默认不参与构建。例如 `pi` 模块在 `pom.xml` 中默认处于注释状态，需要手动启用后再构建。
> 3. 想了解各数据库分别支持哪些测试操作（写入 / Q1–Q12 / 验证查询等），请参见 [各数据库支持的测试操作一览](./OperationSupportMatrix-cn.md)。

## 1.1. 测试 InfluxDB v1.x
[快速指引](../influxdb/README.md)

## 1.2. 测试 InfluxDB v2.0
[快速指引](../influxdb-2.0/README.md)

## 1.3. 测试 CnosDB
[快速指引](../cnosdb/README.md)

## 1.4. 测试 KairosDB
[快速指引](../kairosdb/README.md)

## 1.5. 测试 Microsoft SQL Server
[快速指引](../mssqlserver/README.md)

## 1.6. 测试 OpenTSDB
[快速指引](../opentsdb/README.md)

## 1.7. 测试 QuestDB
[快速指引](../questdb/README.md)

## 1.8. 测试 SQLite
[快速指引](../sqlite/README.md)

## 1.9. 测试 TDengine
[快速指引](../tdengine/README.md)

## 1.10. 测试 TDengine 3.x
[快速指引](../tdengine-3.0/README.md)

## 1.11. 测试 Victoriametrics
[快速指引](../victoriametrics/README.md)

## 1.12. 测试 TimeScaleDB
[快速指引](../timescaledb/README.md)

## 1.13. 测试 TimeScaleDB-Cluster
[快速指引](../timescaledb-cluster/README.md)

## 1.14. 测试 PI Archive

[快速指引](../pi/README.md)

## DolphinDB

DolphinDB v2.x 和 v3.x 适配，分别由 `dolphindb-2.0`（Java API `2.00.11.1`）和 `dolphindb-3.0`（Java API `3.00.0.2`）两个模块支持。写入使用 `MultithreadedTableWriter`（DolphinDB 官方推荐的高吞吐 Java API），查询走原生 `DBConnection.run()` 接口。建表为单宽表 `device_data`，DFS 复合分区：

- **一级**：`RANGE(ts)`，默认 7 天一个分区（`DOLPHINDB_PARTITION_DAYS`）
- **二级**：`HASH([SYMBOL, 1000])` on `deviceId`（`DOLPHINDB_DEVICE_HASH_BUCKETS`）

### 通过 Docker 启动本地 DolphinDB

DolphinDB v3.x（搭配 `dolphindb-3.0` 模块）：
```bash
# Apple Silicon
docker pull --platform linux/arm64 dolphindb/dolphindb:v3.00.5
# Intel
# docker pull dolphindb/dolphindb:v3.00.5

docker run -d --name ddb \
  -p 8848:8848 \
  --ulimit nofile=65536:65536 \
  dolphindb/dolphindb:v3.00.5
```

DolphinDB v2.x（搭配 `dolphindb-2.0` 模块）：
```bash
docker pull --platform linux/arm64 dolphindb/dolphindb:v2.00.18
docker run -d --name ddb \
  -p 8848:8848 \
  --ulimit nofile=65536:65536 \
  dolphindb/dolphindb:v2.00.18
```

Web GUI: `http://127.0.0.1:8848`（默认 `admin` / `123456`）。

### 关键配置

v3.x 用 `DB_SWITCH=DolphinDB-3`，v2.x 用 `DB_SWITCH=DolphinDB-2`：

```properties
DB_SWITCH=DolphinDB-3
HOST=127.0.0.1
PORT=8848
USERNAME=admin
PASSWORD=123456
DB_NAME=benchmark
DOLPHINDB_PARTITION_DAYS=7
DOLPHINDB_DEVICE_HASH_BUCKETS=1000
```

### 注意事项

- DolphinDB 社区版单节点 8GB 内存上限。更大规模 benchmark 需申请企业版授权。
- `DOLPHINDB_DEVICE_HASH_BUCKETS` 默认 1000 对齐 DolphinDB 官方 IoT 示例。设备数极少时可调小（例如 100）以降低元数据开销。
- RANGE 分区边界由 `START_TIME` 加 `LOOP × BATCH_SIZE_PER_WRITE × POINT_STEP` 计算并向上对齐到下一个 `DOLPHINDB_PARTITION_DAYS` 桶。
