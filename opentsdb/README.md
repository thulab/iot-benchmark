# OpenTSDB in benchmark

This document is about how to install and deploy OpenTSDB, and how to test its insertion or query performance with IoT Benchmark.

## 1. Environment

### Prerequisites
OpenTSDB should be installed in a Linux system. The following prerequisites should be installed:

- JDK `>= 1.6`
- Gnuplot `>= 4.2`
- ZooKeeper
- HBase `>= 0.92`

For the benchmark side, also prepare:

- Java 8
- Maven 3.6+

### Installation of the prerequisites

1. JDK

	environment parameter $JAVA_HOME should be set.

2. Gnuplot

Use the following command to install Gnuplot:

```shell
sudo apt-get install gnuplot
```

3. ZooKeeper

(1) Download and decompress the installation file of ZooKeeper, rename the file `conf/zoo_sample.cfg` to `zoo.cfg`, for example:

```shell
cp conf/zoo_sample.cfg conf/zoo.cfg
```

(2) Modify the `dataDir` parameter in the file `conf/zoo.cfg` to an accessible path.

(3) Use `bin/zkServer.sh start` to start ZooKeeper. If the process is running successfully, you can see the `QuorumPeerMain` process in `jps`.

(4) Use `bin/zkServer.sh stop` to stop ZooKeeper under the root directory.

4. HBase

(1) Download and decompress the installation file of HBase, move into the file `conf/hbase-env.sh`, modify `HBASE_MANAGES_ZK` to `false`. Then move into the file `conf/hbase-site.xml` and add the following code in `<configuration></configuration>`:

```xml
<property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
</property>
```

(2) Ensure the ZooKeeper process is running, and run `bin/start-hbase.sh` to start HBase.
You can see the `HMaster` and `HRegionServer` processes in `jps` while HBase is running. You can also enter the `bin` folder and use `./hbase shell` to query the data in HBase.

(3) Use `bin/stop-hbase.sh` to stop HBase.

## 2. Database setup

1. Download and decompress the installation file of OpenTSDB, create the `build` directory and move `third_party` into it, for example:

```shell
mkdir build
cp -r third_party ./build
```

2. Build OpenTSDB:

```shell
./build.sh
```

3. Create the necessary tables in HBase by script:

```shell
Env COMPRESSION=NONE HBASE_HOME=/xxx/hbase-x.x.x ./src/create_table.sh
```

4. Make the configuration of OpenTSDB:

(1) Create a configuration in the `build` directory using the template:

```shell
mv src/opentsdb.conf build/opentsdb.conf
```

(2) Modify some parameter values in `build/opentsdb.conf`:

- modify `tsd.network.port` to `4242`
- modify `tsd.http.staticroot` to `./staticroot`
- modify `tsd.http.cachedir` to an accessible path used to store the cache

5. There are a couple of default OpenTSDB properties that will block the test. Therefore, we need to modify the configuration file:

```properties
# modify
tsd.core.auto_create_metrics = true

# add
tsd.http.request.enable_chunked = true
tsd.http.request.max_chunk = 65535
tsd.storage.fix_duplicates=true
```

6. Ensure the tables are created and ZooKeeper and HBase are running. Then start OpenTSDB in the `build` directory:

```shell
./tsdb tsd
```

The `jps` process name of OpenTSDB is `TSDMain`.

Notes specific to this module:

- The benchmark connects to the OpenTSDB HTTP API.
- This module does not require a separate benchmark database or schema creation step.
- `CREATE_SCHEMA=true` does not create extra schema objects for this module.
- `IS_DELETE_DATA=true` issues deletion requests for benchmark metric groups. Use it carefully on shared environments.

## 3. Build benchmark

Build only the OpenTSDB module and its dependencies:

```bash
mvn -pl opentsdb -am package -DskipTests
```

This command has been verified locally in this repository.

After packaging, the benchmark tool is generated under:

```text
opentsdb/target/iot-benchmark-opentsdb
opentsdb/target/iot-benchmark-opentsdb.zip
```

## 4. Configure benchmark

When using benchmark for testing, there are some differences between OpenTSDB and other databases.

This module does not provide a dedicated module-local `config.properties` file. Use the shared benchmark configuration file, such as `configuration/conf/config.properties` or the packaged `conf/config.properties`.

For the current `opentsdb` module, check at least the following items:

| Key                          | Required | Description                                                                                                                                        |
| :--------------------------- | :------- | :------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DB_SWITCH`                  | Yes      | Must be `OpenTSDB`.                                                                                                                                |
| `HOST`                       | Yes      | Target OpenTSDB host. This module currently uses only the first host, and the value should include the protocol prefix such as `http://127.0.0.1`. |
| `PORT`                       | Yes      | Target OpenTSDB HTTP API port. The common OpenTSDB setting is `4242`.                                                                              |
| `INSERT_DATATYPE_PROPORTION` | Yes      | OpenTSDB does not support boolean or text data in this module. Use `0:1:1:1:1:0`.                                                                  |
| `DB_NAME`                    | No       | Present in the shared configuration model, but not used by the current `opentsdb` module.                                                          |
| `USERNAME`                   | No       | Present in the shared configuration model, but not used by the current `opentsdb` module.                                                          |
| `PASSWORD`                   | No       | Present in the shared configuration model, but not used by the current `opentsdb` module.                                                          |

The original README used a `DB_URL` style description. For the current code in this repository, use `HOST` and `PORT` instead:

```properties
DB_SWITCH=OpenTSDB
HOST=http://your-server-path
PORT=4242
INSERT_DATATYPE_PROPORTION=0:1:1:1:1:0
```

In addition, because of the current OpenTSDB query capability in this module:

- `OPERATION_PROPORTION` should keep the value-filter operations disabled
- `INSERT_DATATYPE_PROPORTION` should not enable boolean or text data

The current `opentsdb` module does **not** support the following benchmark features:

- `verificationQueryMode`
- comparison or verification paths that require framework verification support, including `IS_COMPARISON=true` and `IS_POINT_COMPARISON=true`
- `VALUE_RANGE`, `AGG_VALUE`, and `AGG_RANGE_VALUE` in `OPERATION_PROPORTION`
- `RANGE_QUERY_DESC`, `VALUE_RANGE_QUERY_DESC`, and `GROUP_BY_DESC` in `OPERATION_PROPORTION`
- `SET_OPERATION` in `OPERATION_PROPORTION`
- `ALIGN_BY_DEVICE=true`
- `RESULT_ROW_LIMIT >= 0`
- boolean and text insertion in `INSERT_DATATYPE_PROPORTION`

## 5. Run benchmark

After finishing the configuration, run the benchmark with the packaged scripts:

```bash
cd opentsdb/target/iot-benchmark-opentsdb
./benchmark.sh
```

If you want to run with a custom configuration directory or file path:

```bash
./benchmark.sh -cf conf
```

For a normal mixed read/write benchmark, use `BENCHMARK_WORK_MODE=testWithDefaultPath` and keep unsupported operations disabled.

The test result will be printed in the console and recorded in the generated `logs` directory during execution.

## 6. Test result

The current directory does not contain a longer benchmark output sample in the previous README. After running `./benchmark.sh`, the benchmark result will be shown in the console and written into the generated `logs` directory.
