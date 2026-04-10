# 1. Developer Guide

This page describes the current code structure of IoT Benchmark and the recommended way to extend it.

## 1.1. Repository structure

The current repository is organized around one shared `core` module and a set of database-specific Maven subprojects.

```text
iot-benchmark/
├── core/                    # shared benchmark framework
├── configuration/           # packaged scripts and shared runtime config
├── verification/            # double-write / verification packaging module
├── influxdb/                # one database module
├── cnosdb/                  # one database module
├── timescaledb/             # one database module
├── ...
└── pom.xml                  # root aggregator
```

In the current codebase:

1. Shared benchmark logic lives in `core`.
2. Each tested database is implemented in its own Maven module.
3. The root [pom.xml](../pom.xml) decides which modules participate in the default build.
4. Packaged benchmark distributions reuse the shared files under [configuration](../configuration), rather than packaging module-local configs directly.

## 1.2. Runtime flow

The main runtime path is:

1. [App.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/App.java) parses startup arguments and selects a benchmark mode.
2. [ConfigDescriptor.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/ConfigDescriptor.java) loads `config.properties` from the directory passed by `-cf` or from `configuration/conf` by default.
3. [BenchmarkMode.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/mode/enums/BenchmarkMode.java) maps `BENCHMARK_WORK_MODE` to one of the currently supported modes.
4. The selected mode class under [core/src/main/java/cn/edu/tsinghua/iot/benchmark/mode](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/mode) drives data generation, querying, verification, and measurement.
5. [DBFactory.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java) creates the database adapter according to `DB_SWITCH`.
6. The adapter implements [IDatabase.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/IDatabase.java), which is the core extension contract for all database modules.

For most database-extension work, the files you will touch first are:

1. [IDatabase.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/IDatabase.java)
2. [DBSwitch.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBSwitch.java)
3. [DBType.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBType.java)
4. [Constants.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/Constants.java)
5. [DBFactory.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java)

## 1.3. Core extension points

### 1.3.1. Database adapter contract

[IDatabase.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/IDatabase.java) defines the common adapter surface expected by the framework.

Every database adapter must provide:

1. lifecycle methods: `init`, `cleanup`, `close`
2. schema registration: `registerSchema`
3. ingestion: `insertOneBatch`
4. common query operations such as `preciseQuery`, `rangeQuery`, `valueRangeQuery`, `aggRangeQuery`, `aggValueQuery`, `aggRangeValueQuery`, `groupByQuery`, `latestPointQuery`, `rangeQueryOrderByDesc`, `valueRangeQueryOrderByDesc`

Several operations are optional at the interface level and already have default fallback behavior:

1. `groupByQueryOrderByDesc`
2. `setOpQuery`
3. `verificationQuery`
4. `deviceQuery`
5. `deviceSummary`

If your target database does not support one of these operations, keep the default unsupported behavior or override it explicitly with a clear failure path.

### 1.3.2. DB switch registration

`DB_SWITCH` values come from [DBSwitch.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBSwitch.java).

The string seen in `config.properties` is generated from:

1. [DBType.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBType.java)
2. optional [DBVersion.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBVersion.java)
3. optional [DBInsertMode.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBInsertMode.java)

Examples already present in the code:

1. `IoTDB-200-SESSION_BY_TABLET`
2. `InfluxDB`
3. `InfluxDB-2.x`
4. `TimescaleDB`
5. `CnosDB`

### 1.3.3. Factory wiring

[DBFactory.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java) resolves the configured `DB_SWITCH` to a Java class name stored in [Constants.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/Constants.java), then instantiates that class reflectively with a single-argument constructor:

```java
public YourDatabase(DBConfig dbConfig)
```

This constructor shape is required by the current factory implementation.

### 1.3.4. Shared configuration loading

[ConfigDescriptor.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/ConfigDescriptor.java) loads and validates the benchmark configuration.

Two practical implications matter when you add a new database:

1. The packaged runtime uses the shared [configuration/conf/config.properties](../configuration/conf/config.properties), because module packaging copies `configuration/conf` through `src/assembly/assembly.xml`.
2. A module-local `config.properties` file, such as [influxdb/config.properties](../influxdb/config.properties), is useful as sample documentation, but it is not the packaged runtime config by default.

## 1.4. How to run from an IDE

The current project keeps lightweight IDE entry classes in database modules so you can run `App.main(args)` directly from the target module.

Examples:

1. [influxdb/src/main/test/cn/edu/tsinghua/iot/benchmark/InfluxDBTestEntrance.java](../influxdb/src/main/test/cn/edu/tsinghua/iot/benchmark/InfluxDBTestEntrance.java)
2. [iotdb-2.0/src/main/test/cn/edu/tsinghua/iot/benchmark/IoTDB200TestEntrance.java](../iotdb-2.0/src/main/test/cn/edu/tsinghua/iot/benchmark/IoTDB200TestEntrance.java)
3. [cnosdb/src/test/cn/edu/tsinghua/iot/benchmark/CnosDBTestEntrance.java](../cnosdb/src/test/cn/edu/tsinghua/iot/benchmark/CnosDBTestEntrance.java)

The repository is not fully consistent here: some modules use `src/main/test`, while others use `src/test`.

For IDE runs:

1. choose the entry class in the module you are working on
2. pass `-cf <config-dir>` if you want a custom config directory
3. otherwise the benchmark falls back to `configuration/conf`

## 1.5. How to add a new tested database

This is the minimum code path required to onboard a new database module cleanly.

### 1.5.1. Create the module

Add a new Maven subproject at the repository root, for example:

```text
yourdb/
├── pom.xml
├── README.md
├── config.properties
└── src/
    ├── assembly/
    │   └── assembly.xml
    ├── main/
    │   ├── java/
    │   │   └── cn/edu/tsinghua/iot/benchmark/yourdb/
    │   │       └── YourDB.java
    │   └── resources/
    │       └── log4j.properties
    └── test/ or main/test/
        └── cn/edu/tsinghua/iot/benchmark/
            └── YourDBTestEntrance.java
```

Use an existing simple module as the starting point. Good current references are:

1. [influxdb](../influxdb)
2. [cnosdb](../cnosdb)
3. [timescaledb](../timescaledb)

Then register the module in the root [pom.xml](../pom.xml) `<modules>` list.

### 1.5.2. Add the module `pom.xml`

At minimum, the new module should:

1. inherit from the root `iot-benchmark` parent
2. depend on `core`
3. include the driver/client dependencies needed by the target database
4. configure `maven-assembly-plugin`
5. produce a final package name like `iot-benchmark-yourdb`

The current packaging pattern is consistent across database modules: the assembled distribution includes the shared scripts and the shared `configuration/conf` directory.

### 1.5.3. Implement the adapter class

Create a database adapter class that implements [IDatabase.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/IDatabase.java).

Required rules from the current factory and runtime:

1. provide a public constructor taking `DBConfig`
2. implement lifecycle methods and ingestion/query operations supported by your database
3. return a `Status` object rather than throwing uncontrolled exceptions from benchmark operations
4. respect the current shared config model from [ConfigDescriptor.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/ConfigDescriptor.java)

You do not have to support every optional operation. The current codebase already has modules with partial support.

Two implementation styles already exist:

1. direct implementation, for example [influxdb/InfluxDB.java](../influxdb/src/main/java/cn/edu/tsinghua/iot/benchmark/influxdb/InfluxDB.java)
2. reuse by extending another adapter, for example [cnosdb/CnosDB.java](../cnosdb/src/main/java/cn/edu/tsinghua/iot/benchmark/cnosdb/CnosDB.java)

If your target database is close to an existing protocol or SQL dialect, reusing an existing adapter usually reduces maintenance cost.

### 1.5.4. Register the new `DB_SWITCH`

To make the new adapter reachable from configuration:

1. add a new `DBType` in [DBType.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBType.java) if needed
2. add a new `DBVersion` only if the module must expose a version in `DB_SWITCH`
3. add a new `DBInsertMode` only if the module needs multiple insertion styles in `DB_SWITCH`
4. add a new entry in [DBSwitch.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBSwitch.java)
5. add the adapter class name constant in [Constants.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/Constants.java)
6. add the corresponding branch in [DBFactory.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java)

Without these changes, the framework cannot construct the new database adapter from `config.properties`.

### 1.5.5. Decide what operations are really supported

Before documenting the new module, decide which benchmark features are truly supported by the adapter.

At minimum, check these areas against the code you wrote:

1. ingestion
2. schema creation and cleanup
3. precise query
4. range query
5. value-range query
6. aggregation query families
7. `GROUP_BY_DESC`
8. `SET_OPERATION`
9. `verificationQueryMode`
10. dual-database verification paths such as `deviceQuery` and `deviceSummary`
11. `TIMESTAMP_PRECISION`
12. `ALIGN_BY_DEVICE`
13. `RESULT_ROW_LIMIT`

Do not claim support in README unless the adapter code really implements it.

### 1.5.6. Add an IDE entry class

Add a tiny test entrance that forwards to `App.main(args)`, following the existing pattern:

```java
package cn.edu.tsinghua.iot.benchmark;

import java.sql.SQLException;

public class YourDBTestEntrance {
  public static void main(String[] args) throws SQLException {
    App.main(args);
  }
}
```

The repository currently mixes `src/test` and `src/main/test`. Prefer matching the local pattern of the module you copy from, unless you are cleaning that module up at the same time.

### 1.5.7. Add assembly packaging

Create `src/assembly/assembly.xml` by following an existing module.

The current assembly pattern should:

1. package the module dependencies into `lib`
2. copy shared scripts from [configuration/bin](../configuration/bin)
3. copy shared runtime config from [configuration/conf](../configuration/conf)
4. include `benchmark.sh`, `benchmark.bat`, `rep-benchmark.sh`, `cli-benchmark.sh`, `routine`, and `LICENSE`

This is why updating the shared config template under [configuration/conf/config.properties](../configuration/conf/config.properties) is usually more important than editing only `yourdb/config.properties`.

### 1.5.8. Provide a sample module config

Add a concise `yourdb/config.properties` sample in the module root. Existing modules use this as lightweight documentation for the minimal connection fields.

Keep it focused on target-database-specific basics, for example:

```properties
DB_SWITCH=YourDB
HOST=127.0.0.1
PORT=1234
USERNAME=user
PASSWORD=password
DB_NAME=test
```

If your module needs extra database-specific keys, document them there and in the README.

### 1.5.9. Update user-facing docs

When a new database module is added, at least these documents should be reviewed:

1. [README.md](../README.md) for supported database lists and `DB_SWITCH` tables
2. [docs/DifferentTestDatabase.md](./DifferentTestDatabase.md) for the quick-guide index
3. the new module README itself

If the module has special operational limits, also review:

1. [docs/DifferentTestMode.md](./DifferentTestMode.md)
2. [docs/DifferenttestModeConfig.md](./DifferenttestModeConfig.md)

Only add cross-document claims when they are confirmed by code.

## 1.6. Recommended README format for a new database module

Current module READMEs are most useful when they follow the same structure. A practical format is:

1. Title
2. One-sentence statement of what the module tests
3. `Environment`
4. `Database setup`
5. `Build benchmark`
6. `Configure benchmark`
7. `Run benchmark`
8. `Test result`

For consistency with the newer module READMEs, include:

1. the exact `DB_SWITCH` value
2. minimal required connection keys
3. database-specific semantics of `DB_NAME`, `USERNAME`, `PASSWORD`, `TOKEN`, or custom keys
4. whether only the first `HOST` and `PORT` are used
5. whether `CREATE_SCHEMA=true` creates a database, a schema, a table, or only benchmark-side metadata
6. what `IS_DELETE_DATA=true` really deletes
7. explicit unsupported benchmark features
8. explicit supported advanced features that are easy to misunderstand, such as `verificationQueryMode`, `GROUP_BY_DESC`, `SET_OPERATION`, `ALIGN_BY_DEVICE`, or `RESULT_ROW_LIMIT`

### 1.6.1. README skeleton

You can use the following skeleton and then replace the database-specific details:

~~~md
# Benchmark for YourDB

This module uses IoT Benchmark to test YourDB.

## 1. Environment

Before running the benchmark, prepare:

1. Java 8
2. Maven 3.6+
3. A running YourDB instance reachable from the benchmark machine

## 2. Database setup

Explain what must already exist before the benchmark starts, and what `CREATE_SCHEMA` / `IS_DELETE_DATA` do.

## 3. Build benchmark

```bash
mvn -pl yourdb -am package -DskipTests
```

## 4. Configure benchmark

There is a sample configuration file at [config.properties](./config.properties).

List the required keys and a minimal example.

Also list unsupported features explicitly.

## 5. Run benchmark

```bash
cd yourdb/target/iot-benchmark-yourdb
./benchmark.sh
```

## 6. Test result

Provide one real output sample if possible.
~~~

## 1.7. Verification checklist before opening a PR

Before considering a new database module complete, verify at least:

1. the module is added to [pom.xml](../pom.xml) if it should participate in default builds
2. `DB_SWITCH` can be parsed from `config.properties`
3. [DBFactory.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java) can instantiate the adapter
4. the module packages successfully with `mvn -pl yourdb -am package -DskipTests`
5. the packaged directory contains `bin`, `conf`, `lib`, and startup scripts
6. the README documents the real supported and unsupported operations
7. if the module supports verification, the README states which verification path is supported
8. [docs/DifferentTestDatabase.md](./DifferentTestDatabase.md) is updated if the module should be discoverable from the docs index

## 1.8. Notes on current codebase conventions

A few conventions in the current repository are important when extending it:

1. Not every module follows exactly the same source layout.
2. The shared runtime config is centralized under `configuration/conf`.
3. Adapter instantiation is reflection-based, so constructor signatures and class-name constants matter.
4. Partial feature support is normal, but it must be documented precisely.
5. The quickest safe way to add a new module is to copy the closest existing module, then trim or extend behavior from there.
