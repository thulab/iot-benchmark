Benchmark for timescaleDB
---

## 1. Environment

Before running the benchmark, prepare:

1. A running TimescaleDB multi-node or cluster deployment
2. Java 8
3. Maven 3.6+

The sample configuration in this directory uses:

- `DB_SWITCH=TimescaleDB-Cluster`
- `HOST=127.0.0.1`
- `PORT=5432`
- `USERNAME=postgres`
- `PASSWORD=postgres`
- `DB_NAME=postgres`
- `TIMESCALEDB_REPLICATION_FACTOR=1`

For this module, the connected database must already be able to create and use distributed hypertables.

## 2. Database setup

Before running the benchmark:

1. Make sure the PostgreSQL database specified by `DB_NAME` already exists.
2. Make sure the TimescaleDB cluster has been prepared in advance and can execute `create_distributed_hypertable(...)`.
3. Make sure the benchmark user has permission to create tables and query data in that database.
4. Set `TIMESCALEDB_REPLICATION_FACTOR` according to your cluster deployment.

Additional notes for this module:

- `CREATE_SCHEMA=true` creates a benchmark table in the existing PostgreSQL database and converts it to a distributed hypertable.
- `CREATE_SCHEMA=true` does not create the PostgreSQL database itself.
- `IS_DELETE_DATA=true` drops the benchmark table created by this module in the connected database.
- In the current implementation, `DB_NAME` is used both as the JDBC database name and as the benchmark table name created inside that database.

## 3. Build benchmark

Build only the TimescaleDB cluster module and its dependencies:

```bash
mvn -pl timescaledb-cluster -am package -DskipTests
```

This command has been verified locally in this repository.

After packaging, the benchmark tool is generated under:

```text
timescaledb-cluster/target/iot-benchmark-timescaledb-cluster
timescaledb-cluster/target/iot-benchmark-timescaledb-cluster.zip
```

## 4. Configure benchmark

[Demo config](config.properties)

For the current `timescaledb-cluster` module, check at least the following items:

| Key                              | Required | Description                                                                                         |
| :------------------------------- | :------- | :-------------------------------------------------------------------------------------------------- |
| `DB_SWITCH`                      | Yes      | Must be `TimescaleDB-Cluster`.                                                                      |
| `HOST`                           | Yes      | Target PostgreSQL or TimescaleDB access address. The current module uses the first configured host. |
| `PORT`                           | Yes      | Target PostgreSQL port. The sample uses `5432`.                                                     |
| `USERNAME`                       | Yes      | Database username.                                                                                  |
| `PASSWORD`                       | Yes      | Database password.                                                                                  |
| `DB_NAME`                        | Yes      | Existing PostgreSQL database name. In this module it is also used as the benchmark table name.      |
| `TIMESCALEDB_REPLICATION_FACTOR` | Yes      | Replication factor used when the module creates the distributed hypertable.                         |

Minimal example:

```properties
DB_SWITCH=TimescaleDB-Cluster
HOST=127.0.0.1
PORT=5432
USERNAME=postgres
PASSWORD=postgres
DB_NAME=postgres
TIMESCALEDB_REPLICATION_FACTOR=1
```

Notes for TimescaleDB Cluster configuration:

- `DB_NAME` should be an existing database that the benchmark can connect to.
- This module also creates and drops a benchmark table with the same name as `DB_NAME`.
- `TIMESCALEDB_REPLICATION_FACTOR` is specific to this module and affects distributed hypertable creation.
- Other workload parameters such as `CLIENT_NUMBER`, `LOOP`, `BATCH_SIZE_PER_WRITE`, `OPERATION_PROPORTION`, and `QUERY_INTERVAL` are inherited from the global benchmark configuration template under `configuration/conf/config.properties`.

The current `timescaledb-cluster` module does **not** support the following benchmark features:

- `verificationQueryMode`
- comparison or verification paths that require framework verification support, including `IS_COMPARISON=true` and `IS_POINT_COMPARISON=true`
- `GROUP_BY_DESC` in `OPERATION_PROPORTION`
- `SET_OPERATION` in `OPERATION_PROPORTION`
- `ALIGN_BY_DEVICE=true`
- `RESULT_ROW_LIMIT >= 0`

## 5. Run benchmark

Run the benchmark with the packaged scripts:

```bash
cd timescaledb-cluster/target/iot-benchmark-timescaledb-cluster
./benchmark.sh
```

If you want to run with a custom configuration directory or file path:

```bash
./benchmark.sh -cf conf
```

For a normal mixed read/write benchmark, use `BENCHMARK_WORK_MODE=testWithDefaultPath` and keep the unsupported options above disabled.

## 6. Test result

This directory currently does not include a dedicated sample result block.

After running the benchmark, the test result is printed in the console and recorded under the generated `logs` directory in the packaged benchmark folder.
