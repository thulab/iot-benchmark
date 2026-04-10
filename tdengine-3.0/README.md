# Benchmark for TDengine

## 1. Environment

Before running the benchmark, prepare:

1. A running TDengine 3.x server
2. Java 8
3. Maven 3.6+
4. A benchmark machine that can access the TDengine server

The sample configuration in this directory uses:

- `DB_SWITCH=TDengine-3`
- `HOST=127.0.0.1`
- `PORT=6030`
- `USERNAME=root`
- `PASSWORD=taosdata`
- `DB_NAME=test`

For this module, make sure the address configured in `HOST` can be resolved and reached from the benchmark machine.

## 2. Database setup

NOTICE: please create database before test.

In the current `tdengine-3.0` module, keep the following behavior in mind:

1. If you run with `CREATE_SCHEMA=false`, the database named by `DB_NAME` must already exist before the benchmark starts.
2. If you run with `CREATE_SCHEMA=true`, the benchmark can create the database and schema automatically.
3. If you run with `IS_DELETE_DATA=true`, the benchmark drops the database named by `DB_NAME` before the test starts.
4. The current module requires `SENSOR_NUMBER <= 1024`.

The sample connection parameters for database setup are:

```properties
HOST=127.0.0.1
PORT=6030
USERNAME=root
PASSWORD=taosdata
DB_NAME=test
```

## 3. Build benchmark

Build only the TDengine 3.0 module and its dependencies:

```bash
mvn -pl tdengine-3.0 -am package -DskipTests
```

This command has been verified locally in this repository.

After packaging, the benchmark tool is generated under:

```text
tdengine-3.0/target/iot-benchmark-tdengine-3.0
tdengine-3.0/target/iot-benchmark-tdengine-3.0.zip
```

## 4. Configure benchmark

There is a [sample configuration file](./config.properties).

For the current `tdengine-3.0` module, check at least the following items:

| Key                  | Required | Description                                                                                                                      |
| :------------------- | :------- | :------------------------------------------------------------------------------------------------------------------------------- |
| `DB_SWITCH`          | Yes      | Must be `TDengine-3`.                                                                                                            |
| `HOST`               | Yes      | TDengine server address. The current module uses the first configured host.                                                      |
| `PORT`               | Yes      | TDengine JDBC port. The sample uses `6030`.                                                                                      |
| `USERNAME`           | Yes      | TDengine username.                                                                                                               |
| `PASSWORD`           | Yes      | TDengine password.                                                                                                               |
| `DB_NAME`            | Yes      | Benchmark database name used by this module.                                                                                     |
| `TDENGINE_WAL_LEVEL` | No       | WAL level used when the benchmark creates the database. The global sample configuration comments show the default value `2`.     |
| `TDENGINE_REPLICA`   | No       | Replica count used when the benchmark creates the database. The global sample configuration comments show the default value `3`. |

Minimal example:

```properties
DB_SWITCH=TDengine-3
HOST=127.0.0.1
PORT=6030
USERNAME=root
PASSWORD=taosdata
DB_NAME=test
```

Notes for TDengine 3.0 configuration:

- `DB_NAME` is the database operated by this module, not only a logical prefix.
- `TDENGINE_WAL_LEVEL` and `TDENGINE_REPLICA` are specific to this module and are used when the benchmark creates the database automatically.
- Other workload parameters such as `CLIENT_NUMBER`, `LOOP`, `BATCH_SIZE_PER_WRITE`, `OPERATION_PROPORTION`, and `QUERY_INTERVAL` are inherited from the global benchmark configuration template under `configuration/conf/config.properties`.
- This module supports `GROUP_BY_DESC` in `OPERATION_PROPORTION`.
- This module also supports `ALIGN_BY_DEVICE=true` and `RESULT_ROW_LIMIT >= 0`.

The current `tdengine-3.0` module does **not** support the following benchmark features:

- `verificationQueryMode`
- comparison or verification paths that require framework verification support, including `IS_COMPARISON=true` and `IS_POINT_COMPARISON=true`
- `SET_OPERATION` in `OPERATION_PROPORTION`

## 5. Run benchmark

Run the benchmark with the packaged scripts:

```bash
cd tdengine-3.0/target/iot-benchmark-tdengine-3.0
./benchmark.sh
```

If you want to run with a custom configuration directory or file path:

```bash
./benchmark.sh -cf conf
```

For a normal mixed read/write benchmark, use `BENCHMARK_WORK_MODE=testWithDefaultPath`. If `CREATE_SCHEMA=false`, create the target database before starting the benchmark.

## 6. Test result

This directory currently does not include a dedicated sample result block.

After running the benchmark, the test result is printed in the console and recorded under the generated `logs` directory in the packaged benchmark folder.
