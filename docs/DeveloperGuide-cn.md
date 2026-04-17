# 1. 开发者指引

本文档说明 IoT Benchmark 当前的代码结构，以及推荐的扩展方式。

## 1.1. 仓库结构

当前仓库由一个共享的 `core` 模块和一组数据库专用的 Maven 子项目组成。

```text
iot-benchmark/
├── core/                    # 共享 benchmark 框架
├── configuration/           # 打包脚本和共享运行时配置
├── verification/            # 双写 / 校验打包模块
├── influxdb/                # 一个数据库模块
├── cnosdb/                  # 一个数据库模块
├── timescaledb/             # 一个数据库模块
├── ...
└── pom.xml                  # 根聚合工程
```

在当前代码库中：

1. 共享的 benchmark 逻辑位于 `core`。
2. 每一种被测数据库都实现为一个独立的 Maven 模块。
3. 根 [pom.xml](../pom.xml) 决定哪些模块参与默认构建。
4. 打包后的 benchmark 发布物会复用 [configuration](../configuration) 下的共享文件，而不是直接打包每个模块自己的配置。

## 1.2. 运行流程

主要运行链路如下：

1. [App.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/App.java) 解析启动参数并选择 benchmark 运行模式。
2. [ConfigDescriptor.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/ConfigDescriptor.java) 从 `-cf` 传入的目录加载 `config.properties`，如果未传入则默认使用 `configuration/conf`。
3. [BenchmarkMode.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/mode/enums/BenchmarkMode.java) 将 `BENCHMARK_WORK_MODE` 映射为当前支持的运行模式之一。
4. [core/src/main/java/cn/edu/tsinghua/iot/benchmark/mode](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/mode) 下对应的 mode 类负责驱动数据生成、查询、正确性校验和统计。
5. [DBFactory.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java) 根据 `DB_SWITCH` 创建数据库适配器。
6. 数据库适配器实现 [IDatabase.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/IDatabase.java)，它是所有数据库模块的核心扩展接口。

对于大多数数据库扩展工作，最先会改到的文件通常是：

1. [IDatabase.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/IDatabase.java)
2. [DBSwitch.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBSwitch.java)
3. [DBType.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBType.java)
4. [Constants.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/Constants.java)
5. [DBFactory.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java)

## 1.3. 核心扩展点

### 1.3.1. 数据库适配器接口

[IDatabase.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/IDatabase.java) 定义了框架期望的统一数据库适配器接口。

每个数据库适配器都必须提供：

1. 生命周期方法：`init`、`cleanup`、`close`
2. 元数据注册：`registerSchema`
3. 写入：`insertOneBatch`
4. 常用查询操作，例如 `preciseQuery`、`rangeQuery`、`valueRangeQuery`、`aggRangeQuery`、`aggValueQuery`、`aggRangeValueQuery`、`groupByQuery`、`latestPointQuery`、`rangeQueryOrderByDesc`、`valueRangeQueryOrderByDesc`

其中有一些操作在接口层面是可选的，并且已经提供了默认兜底行为：

1. `groupByQueryOrderByDesc`
2. `setOpQuery`
3. `verificationQuery`
4. `deviceQuery`
5. `deviceSummary`

如果目标数据库不支持其中某项操作，可以保持默认的不支持行为，或者显式覆写为清晰的失败路径。

### 1.3.2. `DB_SWITCH` 注册机制

`DB_SWITCH` 的取值来自 [DBSwitch.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBSwitch.java)。

`config.properties` 中看到的字符串由以下几部分拼出来：

1. [DBType.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBType.java)
2. 可选的 [DBVersion.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBVersion.java)
3. 可选的 [DBInsertMode.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBInsertMode.java)

代码中已有的例子包括：

1. `IoTDB-200-SESSION_BY_TABLET`
2. `InfluxDB`
3. `InfluxDB-2.x`
4. `TimescaleDB`
5. `CnosDB`

### 1.3.3. 工厂接线方式

[DBFactory.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java) 会把配置中的 `DB_SWITCH` 解析为 [Constants.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/Constants.java) 中保存的类名，然后通过反射调用一个单参数构造函数来实例化：

```java
public YourDatabase(DBConfig dbConfig)
```

按当前工厂实现，这个构造函数签名是必须满足的。

### 1.3.4. 共享配置加载

[ConfigDescriptor.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/ConfigDescriptor.java) 负责加载并校验 benchmark 配置。

新增数据库时，有两个实际影响需要注意：

1. 打包后的运行时默认使用共享的 [configuration/conf/config.properties](../configuration/conf/config.properties)，因为各模块的打包过程会通过 `src/assembly/assembly.xml` 复制 `configuration/conf`。
2. 模块根目录下自己的 `config.properties`，例如 [influxdb/config.properties](../influxdb/config.properties)，更偏向示例文档用途，默认并不是打包产物中的运行时配置。

## 1.4. 如何从 IDE 运行

当前项目在数据库模块中保留了轻量级的 IDE 入口类，这样可以直接在目标模块中运行 `App.main(args)`。

例如：

1. [influxdb/src/main/test/cn/edu/tsinghua/iot/benchmark/InfluxDBTestEntrance.java](../influxdb/src/main/test/cn/edu/tsinghua/iot/benchmark/InfluxDBTestEntrance.java)
2. [iotdb-2.0/src/main/test/cn/edu/tsinghua/iot/benchmark/IoTDB200TestEntrance.java](../iotdb-2.0/src/main/test/cn/edu/tsinghua/iot/benchmark/IoTDB200TestEntrance.java)
3. [cnosdb/src/test/cn/edu/tsinghua/iot/benchmark/CnosDBTestEntrance.java](../cnosdb/src/test/cn/edu/tsinghua/iot/benchmark/CnosDBTestEntrance.java)

当前仓库在这里并不完全统一：有些模块使用 `src/main/test`，有些模块使用 `src/test`。

对于 IDE 运行：

1. 选择你正在开发模块里的入口类
2. 如果要使用自定义配置目录，传入 `-cf <config-dir>`
3. 如果不传，benchmark 会回退到 `configuration/conf`

## 1.5. 如何新增一种被测数据库

下面是把一个新数据库模块接入 IoT Benchmark 的最小代码路径。

### 1.5.1. 创建模块

在仓库根目录新增一个 Maven 子项目，例如：

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

建议从一个结构相对简单的现有模块复制开始。当前比较适合作为参考的有：

1. [influxdb](../influxdb)
2. [cnosdb](../cnosdb)
3. [timescaledb](../timescaledb)

然后在根 [pom.xml](../pom.xml) 的 `<modules>` 列表中注册该模块。

### 1.5.2. 添加模块 `pom.xml`

最少需要满足以下几点：

1. 继承根工程 `iot-benchmark`
2. 依赖 `core`
3. 加入目标数据库需要的驱动或客户端依赖
4. 配置 `maven-assembly-plugin`
5. 产出类似 `iot-benchmark-yourdb` 这样的最终包名

当前各数据库模块的打包模式基本一致：打出的运行目录会包含共享脚本和共享的 `configuration/conf` 目录。

### 1.5.3. 实现适配器类

创建一个数据库适配器类，实现 [IDatabase.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/IDatabase.java)。

根据当前工厂和运行时约束，至少要满足：

1. 提供一个接收 `DBConfig` 的 `public` 构造函数
2. 实现目标数据库真正支持的生命周期方法、写入和查询操作
3. benchmark 操作中优先返回 `Status`，而不是直接抛出无法控制的异常
4. 遵循 [ConfigDescriptor.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/ConfigDescriptor.java) 当前的共享配置模型

你不需要支持所有可选操作。当前代码库里已经有不少模块是“部分支持”的实现。

现有代码里已经有两种实现风格：

1. 直接实现，例如 [influxdb/InfluxDB.java](../influxdb/src/main/java/cn/edu/tsinghua/iot/benchmark/influxdb/InfluxDB.java)
2. 复用已有适配器再扩展，例如 [cnosdb/CnosDB.java](../cnosdb/src/main/java/cn/edu/tsinghua/iot/benchmark/cnosdb/CnosDB.java)

如果目标数据库和现有协议或 SQL 方言接近，基于已有适配器复用，通常比从零写一套更稳。

### 1.5.4. 注册新的 `DB_SWITCH`

要让新适配器能从配置里真正被创建出来，需要做以下接线：

1. 如有需要，在 [DBType.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBType.java) 中新增 `DBType`
2. 如果该模块需要在 `DB_SWITCH` 中暴露版本号，则在 [DBVersion.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBVersion.java) 中新增版本
3. 如果该模块需要多种写入方式，则在 [DBInsertMode.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBInsertMode.java) 中新增插入模式
4. 在 [DBSwitch.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBSwitch.java) 中新增枚举项
5. 在 [Constants.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/Constants.java) 中新增适配器类名常量
6. 在 [DBFactory.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java) 中新增对应分支

如果这些地方没有补齐，框架就无法从 `config.properties` 正常构造出新数据库适配器。

### 1.5.5. 明确支持哪些操作

在为新模块编写 README 之前，先确认代码层面到底支持哪些 benchmark 能力。

至少建议逐项核对以下内容：

1. 写入
2. 建表或建 schema，以及清理逻辑
3. 精确点查询
4. 时间范围查询
5. 值过滤范围查询
6. 聚合查询族
7. `GROUP_BY_DESC`
8. `SET_OPERATION`
9. `verificationQueryMode`
10. 双库校验链路中的 `deviceQuery` 和 `deviceSummary`
11. `TIMESTAMP_PRECISION`
12. `ALIGN_BY_DEVICE`
13. `RESULT_ROW_LIMIT`

README 里不要声明代码里其实没有实现的能力。

### 1.5.6. 添加 IDE 入口类

新增一个很薄的入口类，直接转发到 `App.main(args)`，模式可以沿用现有写法：

```java
package cn.edu.tsinghua.iot.benchmark;

import java.sql.SQLException;

public class YourDBTestEntrance {
  public static void main(String[] args) throws SQLException {
    App.main(args);
  }
}
```

当前仓库中 `src/test` 和 `src/main/test` 是混用的。除非你同时要清理该模块结构，否则优先沿用你复制来源模块的本地习惯。

### 1.5.7. 增加 assembly 打包配置

创建 `src/assembly/assembly.xml`，可直接参考现有模块。

按当前打包模式，这个 assembly 至少应当：

1. 把模块依赖打入 `lib`
2. 从 [configuration/bin](../configuration/bin) 复制共享脚本
3. 从 [configuration/conf](../configuration/conf) 复制共享运行时配置
4. 包含 `benchmark.sh`、`benchmark.bat`、`rep-benchmark.sh`、`cli-benchmark.sh`、`routine` 和 `LICENSE`

这也是为什么新增模块时，更新共享模板 [configuration/conf/config.properties](../configuration/conf/config.properties) 通常比只改 `yourdb/config.properties` 更重要。

### 1.5.8. 提供模块示例配置

在模块根目录下放一个简洁的 `yourdb/config.properties` 示例文件。当前仓库里的各模块通常都用它来说明最小连接参数。

内容建议聚焦于目标数据库的基础连接项，例如：

```properties
DB_SWITCH=YourDB
HOST=127.0.0.1
PORT=1234
USERNAME=user
PASSWORD=password
DB_NAME=test
```

如果该模块还有额外的数据库专属配置项，也应同时在这个文件和 README 中说明。

### 1.5.9. 更新面向用户的文档

新增数据库模块后，至少建议检查这些文档：

1. [README.md](../README.md)，用于更新支持的数据库列表和 `DB_SWITCH` 对照表
2. [docs/DifferentTestDatabase.md](./DifferentTestDatabase.md)，用于把新模块接入快速指引索引
3. 新模块自己的 README

如果这个模块有特殊限制，也建议额外检查：

1. [docs/DifferentTestMode.md](./DifferentTestMode.md)
2. [docs/DifferenttestModeConfig.md](./DifferenttestModeConfig.md)

只有在代码已经确认支持时，才应把这些能力写入跨文档说明。

## 1.6. 新数据库模块 README 推荐格式

当前模块 README 最有价值的写法，是尽量遵循统一结构。一个实用的格式如下：

1. 标题
2. 一句说明这个模块测试什么数据库
3. `Environment`
4. `Database setup`
5. `Build benchmark`
6. `Configure benchmark`
7. `Run benchmark`
8. `Test result`

为了和当前较新的模块 README 保持一致，建议明确写出：

1. 精确的 `DB_SWITCH` 值
2. 最少必填的连接参数
3. `DB_NAME`、`USERNAME`、`PASSWORD`、`TOKEN` 或自定义参数在该数据库里的具体语义
4. 是否只使用第一组 `HOST` 和 `PORT`
5. `CREATE_SCHEMA=true` 到底是创建数据库、schema、表，还是只做 benchmark 侧的元数据准备
6. `IS_DELETE_DATA=true` 实际删除的是什么
7. 明确列出不支持的 benchmark 功能
8. 明确列出那些容易被误解但实际上支持的高级功能，例如 `verificationQueryMode`、`GROUP_BY_DESC`、`SET_OPERATION`、`ALIGN_BY_DEVICE`、`RESULT_ROW_LIMIT`

### 1.6.1. README 模板

你可以使用下面这份骨架，再替换成目标数据库自己的细节：

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

## 1.7. 提交 PR 前的检查清单

在把一个新数据库模块视为完成前，至少确认以下几点：

1. 如果该模块应参与默认构建，已经把它加入 [pom.xml](../pom.xml)
2. `DB_SWITCH` 能从 `config.properties` 正常解析
3. [DBFactory.java](../core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java) 能实例化该适配器
4. 模块能通过 `mvn -pl yourdb -am package -DskipTests` 正常打包
5. 打包后的目录中包含 `bin`、`conf`、`lib` 和启动脚本
6. README 准确说明了真实支持和不支持的操作
7. 如果模块支持正确性校验，README 已说明支持哪种校验路径
8. 如果需要从文档索引中被发现，已经更新 [docs/DifferentTestDatabase.md](./DifferentTestDatabase.md)

## 1.8. 当前代码库的一些约定

在扩展当前仓库时，有几个约定需要特别注意：

1. 各模块的源码目录结构并不完全统一。
2. 共享运行时配置集中在 `configuration/conf`。
3. 数据库适配器通过反射实例化，因此构造函数签名和类名常量都很关键。
4. “部分支持”是允许的，但必须在文档里写清楚。
5. 新增模块最稳妥的方式，通常是复制一个最接近的现有模块，再在其基础上裁剪或扩展。
