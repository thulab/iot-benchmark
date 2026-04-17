# 1. 自动化脚本

本文档说明 IoT Benchmark 当前打包提供的自动化脚本，以及这些脚本在当前代码里的真实行为。

## 1.1. 脚本结构

当前打包后的 benchmark 发布物会复用 [configuration](../configuration) 下的共享脚本：

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

当前调用链如下：

1. `benchmark.sh` 是 Unix 下的启动入口脚本。
2. `benchmark.sh` 会转调 [startup.sh](../configuration/bin/startup.sh)。
3. `startup.sh` 会启动 `cn.edu.tsinghua.iot.benchmark.App`。
4. `cli-benchmark.sh` 和 `rep-benchmark.sh` 都是对 `benchmark.sh` 的薄封装。

## 1.2. `benchmark.sh` 与 `startup.sh`

对于常规脚本启动，真正的核心逻辑在 [startup.sh](../configuration/bin/startup.sh)。

根据当前代码：

1. `benchmark.sh` 只打印启动横幅，并把所有参数继续传给 `bin/startup.sh`。
2. `startup.sh` 支持 `-cf`、`-heapsize` 和 `-maxheapsize`。
3. 如果没有传 `-cf`，默认配置目录是 `${BENCHMARK_HOME}/conf`。
4. `startup.sh` 会把传入的配置路径转换成绝对路径，并检查该路径是否存在。
5. Java 进程会以 `-Dlogback.configurationFile=${benchmark_conf}/logback.xml` 启动。
6. 当前脚本还会固定传入 `-Duser.timezone=GMT+8`。

例如：

```bash
./benchmark.sh -cf conf -heapsize 1G -maxheapsize 2G
```

在 Windows 下，单次运行入口是 [benchmark.bat](../configuration/benchmark.bat)。而下文提到的自动化封装脚本都是 shell 脚本，实际更偏向 Unix 环境。

## 1.3. 本地一键脚本：`cli-benchmark.sh`

当前的 [cli-benchmark.sh](../configuration/cli-benchmark.sh) 并不是通用自动化框架，而是一个面向本地 IoTDB 测试的窄封装脚本。

### 1.3.1. 它当前会做什么

根据当前脚本实现：

1. 它会从 `${BENCHMARK_HOME}/conf/config.properties` 读取 `DB_SWITCH`。
2. 只有当 `DB_SWITCH` 包含 `IoTDB` 时，它才会执行自动的服务启停逻辑。
3. 在这种情况下，它会删除 `${IOTDB_HOME}/data`，停止本地 IoTDB 服务，短暂等待后重新启动本地 IoTDB 服务。
4. 它会直接原地改写 `${BENCHMARK_HOME}/conf/config.properties`，把 `BENCHMARK_WORK_MODE` 强制设成 `testWithDefaultPath`。
5. 它会在启动 benchmark 前直接原地改写 `${BENCHMARK_HOME}/conf/logback.xml`。
6. 最后它会执行 `./benchmark.sh`。

所以当前脚本的前提假设是：

1. 你当前所在目录是打包后的 benchmark 运行目录。
2. benchmark 配置位于 `conf` 下。
3. 目标是一个由本地脚本管理的 IoTDB 实例。
4. 可以接受对本地 IoTDB 数据目录执行破坏性清理。

### 1.3.2. 使用前必须修改的地方

使用当前脚本前，必须先改：

1. [cli-benchmark.sh](../configuration/cli-benchmark.sh) 中的 `IOTDB_HOME`

当前脚本里写死的是：

```sh
IOTDB_HOME=/home/user/github/iotdb/iotdb/
```

这个路径只是占位值。

### 1.3.3. 当前实现的重要限制

当前实现有几个旧文档没有写清楚的限制：

1. 它是 IoTDB 专用的。对于非 IoTDB 的 `DB_SWITCH`，脚本虽然仍会执行 `benchmark.sh`，但不会启动或停止任何目标数据库。
2. 它总是会把 `BENCHMARK_WORK_MODE` 强制改成 `testWithDefaultPath`。
3. 它会直接修改 `conf/config.properties` 和 `conf/logback.xml`，因此打包目录并不是只读的。
4. 当目标是 IoTDB 时，它会在重启前删除本地 IoTDB 数据目录。
5. 当前共享的 [logback.xml](../configuration/conf/logback.xml) 会把 benchmark 日志写到 `logs/`，并不存在单独的 `server-logs/` 路径。
6. 脚本中使用了 `[[ ... =~ ... ]]`，因此实际要求 shell 兼容这种语法。
7. 脚本里存在 `kill -9 $SERVER_PID` 这一行，但当前脚本并没有给 `SERVER_PID` 赋值，因此这个停止步骤并没有真正接到一个受管的 benchmark 侧服务进程上。

### 1.3.4. 典型用法

在修改好 `IOTDB_HOME` 之后，执行：

```bash
./cli-benchmark.sh
```

按当前共享日志配置，benchmark 日志会写到：

```text
logs/
```

## 1.4. 批量执行脚本：`rep-benchmark.sh`

当前的 [rep-benchmark.sh](../configuration/rep-benchmark.sh) 是一个非常简单的参数改写循环，用于重复执行 benchmark。

### 1.4.1. 它如何工作

根据当前脚本：

1. 它会读取 `${BENCHMARK_HOME}` 下名为 `routine` 的文件。
2. 它使用 shell 的空白分词方式对这个文件进行切分。
3. 每个不是 `TEST` 的 token 都会被当成一次 `KEY=VALUE` 替换。
4. 对于每个这样的 token，它都会用 `sed` 去改写 `${BENCHMARK_HOME}/conf/config.properties` 中对应的那一行。
5. 当读到 `TEST` 这个 token 时，它会执行 `${BENCHMARK_HOME}/benchmark.sh`。

当前默认的 [routine](../configuration/routine) 内容是：

```text
LOOP=1000 TEST
```

### 1.4.2. `routine` 的实际含义

一个典型的 routine 文件可以写成：

```text
LOOP=10 DEVICE_NUMBER=100 TEST
LOOP=20 DEVICE_NUMBER=50 TEST
LOOP=50 DEVICE_NUMBER=20 TEST
```

按当前脚本逻辑，它的含义是：

1. 先替换 `LOOP` 和 `DEVICE_NUMBER`
2. 在读到 `TEST` 时执行一次 benchmark
3. 继续复用已经被改写过的配置，进入下一组 token

### 1.4.3. 当前实现的重要限制

这个脚本本身就设计得很简单，而这种简单性本身就是它的限制：

1. 它是按 shell 空白分词，不是按“每一行是一个完整对象”来解析，所以 `TEST` 本质上只是一个 token。
2. 它只能改写那些已经以未注释 `KEY=...` 形式出现在 `conf/config.properties` 里的配置项。
3. 如果某个配置项不存在，或者仍然是注释状态，脚本不会帮你新增。
4. 它执行完之后不会恢复原始配置。
5. 它不会在启动前校验参数组合是否合法。

### 1.4.4. 运行方式

```bash
./rep-benchmark.sh
```

如果要后台长时间运行：

```bash
./rep-benchmark.sh > /dev/null 2>&1 &
```

按当前共享日志配置，查看进度可以执行：

```bash
cd ./logs
tail -f log_info.log
```

## 1.5. 实际使用建议

在使用当前自动化脚本时，建议记住以下几点：

1. 共享启动链路是稳定的：`benchmark.sh -> startup.sh -> App`。
2. 批量运行是基于配置文件改写，而不是基于更高层的场景对象。
3. `cli-benchmark.sh` 当前是以 IoTDB 为中心写的，不应把它视为通用的数据库无关启动器。
4. 打包后的脚本默认依赖 `configuration/` 对应的共享目录结构保持一致。
5. 如果你的 benchmark 场景有特殊的启动或清理语义，应该明确写在文档里，而不是默认认为共享脚本足够覆盖。
