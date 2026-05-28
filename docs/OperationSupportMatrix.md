# Test Operations Supported by Each Database

This document lists **which test operations each database supports** in IoT Benchmark. Use it when configuring `OPERATION_PROPORTION`, choosing a verification (comparison) mode, or picking a database to benchmark.

## 1. The test operations

Ingestion plus 12 queries (Q1–Q12) can be mixed by ratio via `OPERATION_PROPORTION`. Dataset verification and dual-DB comparison are driven by the dedicated verification modes and are not part of `OPERATION_PROPORTION`.

`OPERATION_PROPORTION` is a colon-separated list of **13 positions**: `WRITE:Q1:Q2:Q3:Q4:Q5:Q6:Q7:Q8:Q9:Q10:Q11:Q12`.

| Pos | Short name | Meaning |
|---|---|---|
| 1 | Write | Batch data ingestion |
| 2 | Q1 Precise point | `... where time = ? and device in ?` |
| 3 | Q2 Time range | `... where time >= ? and time <= ? and device in ?` |
| 4 | Q3 Range + value filter | `... and v1 > ?` |
| 5 | Q4 Agg with time filter | `select func(v1)... where time >= ? and time <= ?` |
| 6 | Q5 Agg with value filter | `select func(v1)... where value > ?` |
| 7 | Q6 Agg with time + value filter | `select func(v1)... where time...and value > ?` |
| 8 | Q7 Group-by aggregation | `... group by time(...)` |
| 9 | Q8 Latest point | `select last/max(time)...` |
| 10 | Q9 Range, time desc | Q2 + `order by time desc` |
| 11 | Q10 Value range, time desc | Q3 + `order by time desc` |
| 12 | Q11 Group-by, time desc | Q7 + `order by time desc` |
| 13 | Q12 Set operation | `union / intersect / except` |

Two correctness-check operations are also available (driven by the verification modes):

- **Dataset verification**: write the generated dataset back and verify it point by point — `verificationQueryMode`.
- **Dual-DB comparison**: compare results between two databases point-by-point / by function in the `verification` project — `IS_COMPARISON` / `IS_POINT_COMPARISON`.

## 2. Standard read/write operation matrix

✅ supported　❌ not supported　⚠️ runs but has a known limitation

| Database | Write | Q1 Precise | Q2 Range | Q3 Val-range | Q4 Agg-range | Q5 Agg-val | Q6 Agg-range-val | Q7 Group-by | Q8 Latest | Q9 Range-desc | Q10 Val-range-desc | Q11 Group-by-desc | Q12 Set-op |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **IoTDB 1.3** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **IoTDB 2.0 (tree)** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **IoTDB 2.0 (table)** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **InfluxDB v1.x** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **InfluxDB 2.0** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **CnosDB** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ❌ |
| **VictoriaMetrics** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **TimescaleDB** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ |
| **TimescaleDB-Cluster** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **QuestDB** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **SQLite** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **MS SQL Server** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **TDengine (v2.x)** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **DolphinDB-3** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **TDengine 3.0** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **OpenTSDB** | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **KairosDB** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| _PI Archive_ (disabled) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ |
| _IginX_ (disabled) | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ |

> _PI Archive_ and _IginX_ are not part of the default build; enable their modules manually before use. Listed for reference only.

## 3. Correctness-check operation matrix

| Database | Dataset verification (`verificationQueryMode`) | Dual-DB comparison (`verification` project) |
|---|---|---|
| **IoTDB 1.3** | ✅ | ✅ |
| **IoTDB 2.0 (tree)** | ✅ | ✅ |
| **IoTDB 2.0 (table)** | ✅ | ✅ |
| **InfluxDB v1.x** | ✅ | ❌ |
| **CnosDB** | ✅ | ❌ |
| **TimescaleDB** | ✅ | ✅ |
| **TimescaleDB-Cluster** | ✅ | ✅ |
| **InfluxDB 2.0** | ❌ | ❌ |
| **VictoriaMetrics** | ❌ | ❌ |
| **QuestDB** | ❌ | ❌ |
| **SQLite** | ❌ | ❌ |
| **MS SQL Server** | ❌ | ❌ |
| **TDengine (v2.x)** | ❌ | ❌ |
| **TDengine 3.0** | ❌ | ❌ |
| **OpenTSDB** | ❌ | ❌ |
| **KairosDB** | ❌ | ❌ |

## 4. Selection and configuration tips

- **Most complete**: `IoTDB 2.0 (table)` supports every test operation (including set operations, Q12). `IoTDB 1.3` and `IoTDB 2.0 (tree)` miss only the set operation Q12.
- **Q11 (group-by, time desc)**: supported only by `IoTDB` (all versions/modes), `TDengine 3.0`, and `InfluxDB v1.x`.
  - ⚠️ `CnosDB` can run this query, but the results are not actually sorted in descending time order — keep this in mind.
- **Q12 (set operations: union / intersect / except)**: supported only by `IoTDB 2.0 (table)` and `TimescaleDB` (non-cluster); `TimescaleDB-Cluster` does not support it.
- **Dual-DB comparison**: only `IoTDB` (1.3 / 2.0 all modes) and `TimescaleDB` (incl. cluster) can serve as the comparison target; `InfluxDB v1.x` and `CnosDB` support single-DB dataset verification only.
- **OpenTSDB**: does not support value-filter queries (Q3, Q5, Q6) or the descending queries (Q9, Q10), and does not support `boolean` / `text` data types.

## 5. Configuration examples

Write only:

```properties
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0:0:0
```

Write + an even mix of all queries:

```properties
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1:1:1
```

> If a database does not support an operation, set its position to `0`; otherwise that operation is counted as failed at runtime. For `OpenTSDB`, for example, set Q3, Q5, Q6, Q9, and Q10 to 0.

## 6. Related docs

- Per-database quick guides: [DifferentTestDatabase.md](./DifferentTestDatabase.md)
- Per-mode guides: [DifferentTestMode.md](./DifferentTestMode.md)
