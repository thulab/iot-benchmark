# 各数据库支持的测试操作一览

本文档列出 IoT Benchmark 中**每个数据库分别支持哪些测试操作**，便于你在配置 `OPERATION_PROPORTION`、选择验证（对比）模式或挑选被测数据库时参考。

## 1. 测试操作说明

写入与 12 种查询（Q1–Q12）可通过 `OPERATION_PROPORTION` 按比例混合执行；数据集校验与双库对比则由专门的验证模式触发，不参与 `OPERATION_PROPORTION`。

`OPERATION_PROPORTION` 为英文冒号分隔、共 **13 位**，顺序为：`写入:Q1:Q2:Q3:Q4:Q5:Q6:Q7:Q8:Q9:Q10:Q11:Q12`。

| 位置 | 简称 | 含义 |
|---|---|---|
| 1 | 写入 | 批量写入数据 |
| 2 | Q1 精确点查询 | `... where time = ? and device in ?` |
| 3 | Q2 时间范围查询 | `... where time >= ? and time <= ? and device in ?` |
| 4 | Q3 带值过滤的范围查询 | `... and v1 > ?` |
| 5 | Q4 带时间过滤的聚合查询 | `select func(v1)... where time >= ? and time <= ?` |
| 6 | Q5 带值过滤的聚合查询 | `select func(v1)... where value > ?` |
| 7 | Q6 带时间+值过滤的聚合查询 | `select func(v1)... where time...and value > ?` |
| 8 | Q7 分组聚合查询 | `... group by time(...)` |
| 9 | Q8 最近点查询 | `select last/max(time)...` |
| 10 | Q9 倒序时间范围查询 | Q2 + `order by time desc` |
| 11 | Q10 倒序带值过滤范围查询 | Q3 + `order by time desc` |
| 12 | Q11 倒序分组聚合查询 | Q7 + `order by time desc` |
| 13 | Q12 集合操作查询 | `union / intersect / except` |

此外还有两类正确性校验操作（由验证模式触发）：

- **数据集校验**：将生成的数据集写回数据库后逐点校验，对应 `verificationQueryMode`。
- **双库对比**：在 `verification` 项目中对两个数据库逐点 / 按函数比对结果，对应 `IS_COMPARISON` / `IS_POINT_COMPARISON`。

## 2. 标准读写操作支持矩阵

✅ 支持　❌ 不支持　⚠️ 可执行但有已知限制

| 数据库 | 写入 | Q1 精确点 | Q2 范围 | Q3 值过滤范围 | Q4 时间聚合 | Q5 值聚合 | Q6 时间+值聚合 | Q7 分组聚合 | Q8 最近点 | Q9 倒序范围 | Q10 倒序值过滤 | Q11 倒序分组 | Q12 集合操作 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **IoTDB 1.3** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **IoTDB 2.0（tree 树模型）** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **IoTDB 2.0（table 表模型）** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **InfluxDB v1.x** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **InfluxDB 2.0** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **CnosDB** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ❌ |
| **VictoriaMetrics** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **TimescaleDB** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ |
| **TimescaleDB-Cluster** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **QuestDB** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **SQLite** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **MS SQL Server** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **TDengine（v2.x）** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **DolphinDB-3** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **TDengine 3.0** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **OpenTSDB** | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **KairosDB** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| _PI Archive_（未启用） | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ |
| _IginX_（未启用） | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ |

> _PI Archive_ 与 _IginX_ 默认不参与构建，需手动启用对应模块后方可使用，此处仅供参考。

## 3. 正确性校验操作支持矩阵

| 数据库 | 数据集校验（`verificationQueryMode`） | 双库对比（`verification` 项目） |
|---|---|---|
| **IoTDB 1.3** | ✅ | ✅ |
| **IoTDB 2.0（tree）** | ✅ | ✅ |
| **IoTDB 2.0（table）** | ✅ | ✅ |
| **InfluxDB v1.x** | ✅ | ❌ |
| **CnosDB** | ✅ | ❌ |
| **TimescaleDB** | ✅ | ✅ |
| **TimescaleDB-Cluster** | ✅ | ✅ |
| **InfluxDB 2.0** | ❌ | ❌ |
| **VictoriaMetrics** | ❌ | ❌ |
| **QuestDB** | ❌ | ❌ |
| **SQLite** | ❌ | ❌ |
| **MS SQL Server** | ❌ | ❌ |
| **TDengine（v2.x）** | ❌ | ❌ |
| **TDengine 3.0** | ❌ | ❌ |
| **OpenTSDB** | ❌ | ❌ |
| **KairosDB** | ❌ | ❌ |

## 4. 选型与配置建议

- **功能最完整**：`IoTDB 2.0（table 表模型）` 支持全部测试操作（含集合操作 Q12）。`IoTDB 1.3` 与 `IoTDB 2.0（tree）` 仅缺集合操作 Q12。
- **Q11 倒序分组聚合**：仅 `IoTDB`（各版本/模式）、`TDengine 3.0`、`InfluxDB v1.x` 支持。
  - ⚠️ `CnosDB` 可以执行该查询，但结果并未真正按时间倒序排列，使用时需注意。
- **Q12 集合操作（union / intersect / except）**：仅 `IoTDB 2.0 表模型` 与 `TimescaleDB`（非集群版）支持；`TimescaleDB-Cluster` 不支持。
- **双库对比**：仅 `IoTDB`（1.3 / 2.0 全模式）与 `TimescaleDB`（含集群版）可作为双库对比的目标库；`InfluxDB v1.x`、`CnosDB` 仅支持单库数据集校验。
- **OpenTSDB**：不支持带值过滤的查询（Q3、Q5、Q6）与两类倒序查询（Q9、Q10），且不支持 `boolean` / `text` 类型数据。

## 5. 配置示例

仅写入：

```properties
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0:0:0
```

写入 + 各类查询均衡混合：

```properties
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1:1:1
```

> 若某数据库不支持某操作，请将对应位置设为 `0`，否则该操作在运行时会被记为失败。例如对 `OpenTSDB`，应将 Q3、Q5、Q6、Q9、Q10 置 0。

## 6. 相关文档

- 各数据库快速指南：[DifferentTestDatabase-cn.md](./DifferentTestDatabase-cn.md)
- 各运行模式说明：[DifferentTestMode-cn.md](./DifferentTestMode-cn.md)
