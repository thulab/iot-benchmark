# 1. Automation Scripts

This page describes the automation-related scripts that are currently packaged with IoT Benchmark and explains how to keep them working when you add a new tested database.

## 1.1. Script layout

The packaged benchmark distribution currently reuses the shared scripts under [configuration](../configuration):

```text
configuration/
├── benchmark.sh
├── benchmark.bat
├── cli-benchmark.sh
├── rep-benchmark.sh
├── routine
└── bin/
    └── startup.sh
```

The current call chain is:

1. `benchmark.sh` is the Unix entry script.
2. `benchmark.sh` delegates to [startup.sh](../configuration/bin/startup.sh).
3. `startup.sh` starts `cn.edu.tsinghua.iot.benchmark.App`.
4. `cli-benchmark.sh` and `rep-benchmark.sh` are thin wrappers around `benchmark.sh`.

## 1.2. `benchmark.sh` and `startup.sh`

For normal scripted startup, the real logic is in [startup.sh](../configuration/bin/startup.sh).

According to the current code:

1. `benchmark.sh` only prints a banner and forwards all arguments to `bin/startup.sh`.
2. `startup.sh` supports `-cf`, `-heapsize`, and `-maxheapsize`.
3. If `-cf` is not provided, the default configuration directory is `${BENCHMARK_HOME}/conf`.
4. `startup.sh` converts the provided config path to an absolute path and checks that it exists.
5. The Java process is started with `-Dlogback.configurationFile=${benchmark_conf}/logback.xml`.
6. The current script also forces `-Duser.timezone=GMT+8`.

Example:

```bash
./benchmark.sh -cf conf -heapsize 1G -maxheapsize 2G
```

On Windows, the single-run entry is [benchmark.bat](../configuration/benchmark.bat). The automation wrappers described below are shell scripts and are effectively Unix-oriented.

## 1.3. One-click local script: `cli-benchmark.sh`

The current [cli-benchmark.sh](../configuration/cli-benchmark.sh) is not a general-purpose automation framework. It is a narrow helper for local IoTDB-based testing.

### 1.3.1. What it currently does

According to the script implementation:

1. It reads `DB_SWITCH` from `${BENCHMARK_HOME}/conf/config.properties`.
2. It only performs automatic server lifecycle handling when `DB_SWITCH` contains `IoTDB`.
3. In that case, it deletes `${IOTDB_HOME}/data`, stops the local IoTDB server, waits briefly, and starts the local IoTDB server again.
4. It rewrites `${BENCHMARK_HOME}/conf/config.properties` in place so that `BENCHMARK_WORK_MODE=testWithDefaultPath`.
5. It rewrites `${BENCHMARK_HOME}/conf/logback.xml` in place before launching the benchmark.
6. It finally runs `./benchmark.sh`.

So the current script assumes:

1. you are in a packaged benchmark directory
2. the benchmark configuration lives under `conf`
3. the target is a locally managed IoTDB instance
4. destructive cleanup of the local IoTDB data directory is acceptable

### 1.3.2. What you must edit before using it

Before using the current script, you need to update:

1. `IOTDB_HOME` in [cli-benchmark.sh](../configuration/cli-benchmark.sh)

The current script hardcodes:

```sh
IOTDB_HOME=/home/user/github/iotdb/iotdb/
```

This path is only a placeholder.

### 1.3.3. Important current limitations

The current implementation has several limits that the old document did not explain clearly:

1. It is IoTDB-specific. For non-IoTDB `DB_SWITCH` values, the script still runs `benchmark.sh`, but it does not start or stop any target database.
2. It always forces `BENCHMARK_WORK_MODE=testWithDefaultPath`.
3. It mutates `conf/config.properties` and `conf/logback.xml` directly, so the packaged directory is not treated as immutable.
4. It removes the local IoTDB data directory before restart when the target is IoTDB.
5. The current shared [logback.xml](../configuration/conf/logback.xml) writes benchmark logs to `logs/`; there is no separate `server-logs/` path in the current shared config.
6. The script contains `[[ ... =~ ... ]]`, so in practice it expects a shell compatible with that syntax.
7. The line `kill -9 $SERVER_PID` exists, but `SERVER_PID` is not assigned in the current script, so this stop step is not actually wired to a managed benchmark-side server process.

### 1.3.4. Typical usage

After editing `IOTDB_HOME`, run:

```bash
./cli-benchmark.sh
```

With the current shared logging config, benchmark logs are written under:

```text
logs/
```

## 1.4. Batch execution script: `rep-benchmark.sh`

The current [rep-benchmark.sh](../configuration/rep-benchmark.sh) is a simple parameter-rewrite loop for repeated runs.

### 1.4.1. How it works

According to the script:

1. It reads the file named `routine` from `${BENCHMARK_HOME}`.
2. It tokenizes that file using shell word splitting.
3. Every token that is not `TEST` is treated as a `KEY=VALUE` replacement.
4. For each such token, it rewrites the matching line in `${BENCHMARK_HOME}/conf/config.properties` with `sed`.
5. When it sees the token `TEST`, it launches `${BENCHMARK_HOME}/benchmark.sh`.

The current default [routine](../configuration/routine) is:

```text
LOOP=1000 TEST
```

### 1.4.2. Practical meaning of `routine`

A typical routine file can look like:

```text
LOOP=10 DEVICE_NUMBER=100 TEST
LOOP=20 DEVICE_NUMBER=50 TEST
LOOP=50 DEVICE_NUMBER=20 TEST
```

With the current script logic, this means:

1. replace `LOOP` and `DEVICE_NUMBER`
2. run one benchmark when `TEST` is reached
3. continue reusing the mutated config for the next token sequence

### 1.4.3. Important current limitations

The current script is intentionally simple, but that simplicity matters:

1. It splits by shell whitespace, not by logical “line objects”, so `TEST` is really just a token.
2. It only rewrites keys that already appear as uncommented `KEY=...` lines in `conf/config.properties`.
3. If a key is absent or still commented out, the script does not add it.
4. It does not restore the original configuration after finishing.
5. It does not validate whether the parameter combination is legal before launching the run.

### 1.4.4. Run it

```bash
./rep-benchmark.sh
```

For a long-running background job:

```bash
./rep-benchmark.sh > /dev/null 2>&1 &
```

To inspect progress with the current shared logging config:

```bash
cd ./logs
tail -f log_info.log
```

## 1.5. Practical recommendations

When using the current automation scripts, keep these points in mind:

1. The shared startup path is stable: `benchmark.sh -> startup.sh -> App`.
2. Batch runs are config-rewrite based, not scenario-object based.
3. `cli-benchmark.sh` is currently IoTDB-centric and should not be treated as a generic database-agnostic launcher.
4. The packaged scripts expect the shared layout under `configuration/` to remain consistent.
5. If your benchmark scenario has unusual startup or cleanup semantics, document them explicitly instead of assuming the shared wrappers are enough.
