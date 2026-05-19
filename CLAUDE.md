# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

IoT Benchmark is a Java benchmarking tool for evaluating time-series and real-time databases in Industrial IoT scenarios. It supports 15+ databases (IoTDB, InfluxDB, TimescaleDB, TDengine, QuestDB, etc.) via a pluggable module architecture.

## Build Commands

```bash
# Build all modules (skip tests)
mvn clean package -Dmaven.test.skip=true

# Build a specific DB module only
mvn clean package -pl iotdb-2.0 -am -Dmaven.test.skip=true

# Run tests
mvn test

# Run a single test class
mvn test -pl core -Dtest=OperationControllerTest

# Format check (Spotless, Google Java Format)
mvn spotless:check

# Auto-fix formatting
mvn spotless:apply
```

Spotless runs automatically during `validate` phase. On JDK <= 11, spotless is skipped by default (profile `.java-11-below`).

## Running a Benchmark

After building, the distributable is at `<module>/target/iot-benchmark-<module>/iot-benchmark-<module>/`:

```bash
cd iotdb-2.0/target/iot-benchmark-iotdb-2.0/iot-benchmark-iotdb-2.0
# Edit conf/config.properties, then:
./benchmark.sh
# Or with custom config and heap:
./benchmark.sh -cf conf -heapsize 1G -maxheapsize 2G
```

The main class is `cn.edu.tsinghua.iot.benchmark.App`. Config is loaded from `configuration/conf/config.properties` (or the path passed via `-cf`).

## Architecture

### Multi-Module Maven Structure

- **`core`** — Framework: config loading, mode orchestration, client threading, workload generation, measurement aggregation, the `IDatabase` interface. All DB modules depend on core.
- **DB modules** (`iotdb-2.0`, `iotdb-1.3`, `influxdb`, `influxdb-2.0`, `timescaledb`, `tdengine`, etc.) — Each implements `IDatabase` for a specific database. Each module packages into a standalone distributable via `maven-assembly-plugin`.

### Execution Flow

```
App.main()
  → ConfigDescriptor (singleton) loads config.properties
  → Selects BaseMode subclass based on BENCHMARK_WORK_MODE
  → BaseMode.run()
    → SchemaClient threads register schemas (CyclicBarrier sync)
    → DataClient threads execute workload (read/write mix)
      → OperationController selects operation type per OPERATION_PROPORTION
      → DBWrapper delegates to IDatabase implementation
      → Measurement collects latency/throughput metrics
    → Aggregate and output results
```

### Key Abstractions

- **`IDatabase`** (`core/.../tsdb/IDatabase.java`) — Contract all DB adapters implement: `init()`, `registerSchema()`, `insertOneBatch()`, plus ~12 query methods (precise, range, aggregation, group-by, latest-point, etc.).
- **`DBFactory`** (`core/.../tsdb/DBFactory.java`) — Instantiates DB implementations by reflection. Maps `DBSwitch` enum values to class names defined in `Constants.java`.
- **`BaseMode`** (`core/.../mode/BaseMode.java`) — Template method orchestrating schema registration → data client execution → measurement aggregation. Subclasses: `TestWithDefaultPathMode`, `GenerateDataMode`, `VerificationWriteMode`, `VerificationQueryMode`.
- **`DataClient`** (`core/.../client/DataClient.java`) — Abstract Runnable, one per thread. Subclasses: `GenerateDataMixClient` (mixed read/write), `GenerateDataWriteClient`, `RealDataSetWriteClient`, etc.
- **`Config`** (`core/.../conf/Config.java`) — 100+ parameters. `ConfigDescriptor` loads them from `config.properties` via reflection.
- **`Measurement`** (`core/.../measurement/Measurement.java`) — Per-client metrics using TDigest for percentile latency. Merged across clients at end of run.

### Adding a New Database Adapter

1. Create a new Maven module with `core` as dependency.
2. Implement `IDatabase` interface.
3. Add the class name constant to `Constants.java`.
4. Add the `DBSwitch` mapping in `DBFactory.java`.
5. Add the module to root `pom.xml` `<modules>`.
6. Add assembly descriptor at `src/assembly/assembly.xml` (copy from existing module).

### IoTDB 2.0 Module Structure (representative)

The `iotdb-2.0` module uses a strategy pattern:
- **`IoTDB.java`** — Main `IDatabase` implementation, delegates to model and DML strategies.
- **`ModelStrategy/`** — `TreeStrategy` and `TableStrategy` for different IoTDB SQL dialects (`IoTDB_DIALECT_MODE=tree|table`).
- **`DMLStrategy/`** — `JDBCStrategy` and `SessionStrategy` for different insert modes (SESSION_BY_TABLET, SESSION_BY_RECORD, JDBC).

## Configuration

Main config file: `configuration/conf/config.properties`

Key parameters:
- `DB_SWITCH` — Database + version + insert mode (e.g., `IoTDB-200-SESSION_BY_TABLET`)
- `IoTDB_DIALECT_MODE` — `tree` or `table` (IoTDB 2.0 only)
- `BENCHMARK_WORK_MODE` — `testWithDefaultPath`, `generateDataMode`, `verificationWriteMode`, `verificationQueryMode`
- `OPERATION_PROPORTION` — Colon-separated ratios for operation types (write:preciseQuery:rangeQuery:...)
- `DEVICE_NUMBER`, `SENSOR_NUMBER`, `LOOP`, `BATCH_SIZE_PER_WRITE` — Workload scale
- `HOST`, `PORT`, `USERNAME`, `PASSWORD` — Database connection
- `DATA_CLIENT_NUMBER`, `SCHEMA_CLIENT_NUMBER` — Concurrency

## Code Style

- Java 8 source level
- Google Java Format enforced by Spotless
- Import order: `org.apache.iotdb`, default, `javax`, `java`, static
- UNIX line endings

## Adding a New Config Parameter

When adding a new configuration parameter, must complete **all three steps**:

1. **`Config.java`** — 添加字段、getter/setter
2. **`ConfigDescriptor.java`** — 在 `loadProps()` 中从 properties 读取并 set 到 config（否则配置文件中的值不会生效）
3. **`configuration/conf/config.properties`** — 添加注释说明和默认值示例
