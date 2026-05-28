# 1. Testing other databases with IoT Benchmark

This page is an index of quick guides for database modules that are present in the current repository.

> Note:
> 1. The links below point to module-level README files.
> 2. Some modules are not enabled in the default build. For example, the `pi` module is commented out in `pom.xml` and needs to be enabled manually before building.
> 3. For which test operations each database supports (write / Q1–Q12 / verification queries, etc.), see [Test Operations Supported by Each Database](./OperationSupportMatrix.md).

## 1.1. Testing InfluxDB v1.x
[Quick Guide](../influxdb/README.md)

## 1.2. Testing InfluxDB v2.0
[Quick Guide](../influxdb-2.0/README.md)

## 1.3. Testing CnosDB
[Quick Guide](../cnosdb/README.md)

## 1.4. Testing KairosDB
[Quick Guide](../kairosdb/README.md)

## 1.5. Testing Microsoft SQL Server
[Quick Guide](../mssqlserver/README.md)

## 1.6. Testing OpenTSDB
[Quick Guide](../opentsdb/README.md)

## 1.7. Testing QuestDB
[Quick Guide](../questdb/README.md)

## 1.8. Testing SQLite
[Quick Guide](../sqlite/README.md)

## 1.9. Testing TDengine
[Quick Guide](../tdengine/README.md)

## 1.10. Testing TDengine 3.x
[Quick Guide](../tdengine-3.0/README.md)

## 1.11. Testing Victoriametrics
[Quick Guide](../victoriametrics/README.md)

## 1.12. Testing TimeScaleDB
[Quick Guide](../timescaledb/README.md)

## 1.13. Testing TimeScaleDB-Cluster
[Quick Guide](../timescaledb-cluster/README.md)

## 1.14. Testing PI Archive

[Quick Guide](../pi/README.md)

## DolphinDB

DolphinDB v2.x and v3.x integration via two sibling modules (`dolphindb-2.0` uses Java API `2.00.11.1`; `dolphindb-3.0` uses Java API `3.00.0.2`). Writes use `MultithreadedTableWriter` (DolphinDB's recommended high-throughput Java API); queries go through the native `DBConnection.run()` API. Schema is a single DFS partitioned table `device_data` with composite partitioning:

- **Level 1**: `RANGE(ts)` with 7-day granularity by default (`DOLPHINDB_PARTITION_DAYS`)
- **Level 2**: `HASH([SYMBOL, 1000])` on `deviceId` (`DOLPHINDB_DEVICE_HASH_BUCKETS`)

### Start a local DolphinDB via Docker

For DolphinDB v3.x (use with the `dolphindb-3.0` module):
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

For DolphinDB v2.x (use with the `dolphindb-2.0` module):
```bash
docker pull --platform linux/arm64 dolphindb/dolphindb:v2.00.18
docker run -d --name ddb \
  -p 8848:8848 \
  --ulimit nofile=65536:65536 \
  dolphindb/dolphindb:v2.00.18
```

Web GUI: `http://127.0.0.1:8848` (default `admin` / `123456`).

### Key config

For v3.x use `DB_SWITCH=DolphinDB-3`; for v2.x use `DB_SWITCH=DolphinDB-2`:

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

### Notes

- DolphinDB community edition has an 8 GB memory limit per node. For larger benchmarks, request an enterprise license.
- `DOLPHINDB_DEVICE_HASH_BUCKETS` defaults to 1000 to match the DolphinDB official IoT demo. For very small device counts, smaller bucket counts (e.g. 100) may reduce metadata overhead.
- RANGE partition boundaries are computed from `START_TIME` + `LOOP × BATCH_SIZE_PER_WRITE × POINT_STEP` and rounded up to the next `DOLPHINDB_PARTITION_DAYS` bucket.
