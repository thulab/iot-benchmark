# DolphinDB Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `dolphindb-3.0` module to iot-benchmark that adapts DolphinDB v3.x as a benchmark target, using MultithreadedTableWriter for writes and JDBC for queries, validated end-to-end against a local Docker DolphinDB instance.

**Architecture:** New `dolphindb-3.0` Maven module implementing `IDatabase` via DolphinDB Java API jar (`com.dolphindb:dolphindb-javaapi:3.00.0.2`). Schema is a single DFS partitioned table `device_data` with composite partitioning: `RANGE(ts, 7d)` + `HASH([SYMBOL, 1000])` on `deviceId`. Writes go through one MTW instance per client thread (`threadCount=1`, batchSize aligned to IBatch); queries go through standard JDBC. Three core enum changes + Config addition + DBFactory wiring + ConfigDescriptor data-type-allow-list relaxation.

**Tech Stack:** Java 8, Maven, JUnit 4, DolphinDB Java API 3.00.0.2, DolphinDB Server (Docker latest), Google Java Format (Spotless).

**Spec reference:** `docs/superpowers/specs/2026-05-28-dolphindb-support-design.md`

---

## File Structure

**New files (5):**
- `dolphindb-3.0/pom.xml` — Maven module descriptor
- `dolphindb-3.0/src/assembly/assembly.xml` — Distributable layout
- `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java` — IDatabase implementation
- `dolphindb-3.0/src/main/resources/log4j.properties` — Logging config
- `dolphindb-3.0/src/test/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDBTypeMapTest.java` — Unit test for typeMap

**Modified files (14):**
- `pom.xml` (root) — register module
- `core/.../tsdb/enums/DBType.java` — add `DolphinDB`
- `core/.../tsdb/enums/DBVersion.java` — add `DolphinDB_3`
- `core/.../tsdb/enums/DBSwitch.java` — add `DB_DOLPHINDB_3`
- `core/.../tsdb/DBFactory.java` — wire `DB_DOLPHINDB_3` → class
- `core/.../conf/Constants.java` — add `DOLPHINDB3_CLASS`
- `core/.../conf/Config.java` — add 2 fields + getters/setters
- `core/.../conf/ConfigDescriptor.java` — load 2 props + add DolphinDB to data-type exception
- `configuration/conf/config.properties` — add DolphinDB section
- `README.md` / `README-cn.md` — supported DB tables
- `docs/DifferentTestDatabase.md` / `docs/DifferentTestDatabase-cn.md` — DolphinDB usage notes
- `docs/OperationSupportMatrix.md` / `docs/OperationSupportMatrix-cn.md` — capability row

---

## Phase A: Core integration (Tasks 1–8)

### Task 1: Add `DolphinDB` to `DBType` enum

**Files:**
- Modify: `core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBType.java`

- [ ] **Step 1: Edit the enum to add `DolphinDB` between `MSSQLSERVER` and `VictoriaMetrics`**

Open `core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBType.java`. Insert one new line in the enum list:

Replace:
```java
  QuestDB("QuestDB"),
  MSSQLSERVER("MsSqlServer"),
  VictoriaMetrics("VictoriaMetrics"),
```

With:
```java
  QuestDB("QuestDB"),
  MSSQLSERVER("MsSqlServer"),
  VictoriaMetrics("VictoriaMetrics"),
  DolphinDB("DolphinDB"),
```

- [ ] **Step 2: Verify compile**

Run: `mvn compile -pl core -q -DskipTests`
Expected: BUILD SUCCESS with no errors.

- [ ] **Step 3: Commit**

```bash
git add core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBType.java
git commit -m "feat(core): add DolphinDB to DBType enum"
```

---

### Task 2: Add `DolphinDB_3` to `DBVersion` enum

**Files:**
- Modify: `core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBVersion.java`

- [ ] **Step 1: Add the version**

Replace:
```java
  TDengine_3("3");
```

With:
```java
  TDengine_3("3"),
  DolphinDB_3("3");
```

- [ ] **Step 2: Verify compile**

Run: `mvn compile -pl core -q -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBVersion.java
git commit -m "feat(core): add DolphinDB_3 to DBVersion enum"
```

---

### Task 3: Add `DB_DOLPHINDB_3` to `DBSwitch` enum

**Files:**
- Modify: `core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBSwitch.java`

- [ ] **Step 1: Add the switch entry**

Replace:
```java
  DB_QUESTDB(DBType.QuestDB, null, null),
```

With:
```java
  DB_QUESTDB(DBType.QuestDB, null, null),
  DB_DOLPHINDB_3(DBType.DolphinDB, DBVersion.DolphinDB_3, null),
```

- [ ] **Step 2: Verify compile**

Run: `mvn compile -pl core -q -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/enums/DBSwitch.java
git commit -m "feat(core): add DB_DOLPHINDB_3 to DBSwitch enum"
```

---

### Task 4: Add `DOLPHINDB3_CLASS` constant

**Files:**
- Modify: `core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/Constants.java`

- [ ] **Step 1: Add the constant near `QUESTDB_CLASS`**

Replace:
```java
  public static final String QUESTDB_CLASS = "cn.edu.tsinghua.iot.benchmark.questdb.QuestDB";
```

With:
```java
  public static final String QUESTDB_CLASS = "cn.edu.tsinghua.iot.benchmark.questdb.QuestDB";
  public static final String DOLPHINDB3_CLASS =
      "cn.edu.tsinghua.iot.benchmark.dolphindb3.DolphinDB";
```

- [ ] **Step 2: Verify compile**

Run: `mvn compile -pl core -q -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/Constants.java
git commit -m "feat(core): add DOLPHINDB3_CLASS constant"
```

---

### Task 5: Add 2 DolphinDB fields to `Config.java`

**Files:**
- Modify: `core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/Config.java`

- [ ] **Step 1: Add fields after `CNOSDB_SHARD_NUMBER`**

In the field declaration area (around line 347), replace:
```java
  // 被测系统是CnosDB时的参数
  /** the shard number of cnosdb, which affects the parallelism of write and query operations */
  private int CNOSDB_SHARD_NUMBER = 32;
```

With:
```java
  // 被测系统是CnosDB时的参数
  /** the shard number of cnosdb, which affects the parallelism of write and query operations */
  private int CNOSDB_SHARD_NUMBER = 32;

  // 被测系统是DolphinDB时的参数
  /** the partition granularity (in days) of the first-level RANGE(ts) partition */
  private int DOLPHINDB_PARTITION_DAYS = 7;

  /** the bucket count of the second-level HASH(deviceId) partition */
  private int DOLPHINDB_DEVICE_HASH_BUCKETS = 1000;
```

- [ ] **Step 2: Add getters/setters after `setCNOSDB_SHARD_NUMBER`**

In the getter/setter area (around line 1828), replace:
```java
  public void setCNOSDB_SHARD_NUMBER(int CNOSDB_SHARD_NUMBER) {
    this.CNOSDB_SHARD_NUMBER = CNOSDB_SHARD_NUMBER;
  }
```

With:
```java
  public void setCNOSDB_SHARD_NUMBER(int CNOSDB_SHARD_NUMBER) {
    this.CNOSDB_SHARD_NUMBER = CNOSDB_SHARD_NUMBER;
  }

  public int getDOLPHINDB_PARTITION_DAYS() {
    return DOLPHINDB_PARTITION_DAYS;
  }

  public void setDOLPHINDB_PARTITION_DAYS(int DOLPHINDB_PARTITION_DAYS) {
    this.DOLPHINDB_PARTITION_DAYS = DOLPHINDB_PARTITION_DAYS;
  }

  public int getDOLPHINDB_DEVICE_HASH_BUCKETS() {
    return DOLPHINDB_DEVICE_HASH_BUCKETS;
  }

  public void setDOLPHINDB_DEVICE_HASH_BUCKETS(int DOLPHINDB_DEVICE_HASH_BUCKETS) {
    this.DOLPHINDB_DEVICE_HASH_BUCKETS = DOLPHINDB_DEVICE_HASH_BUCKETS;
  }
```

- [ ] **Step 3: Verify compile**

Run: `mvn compile -pl core -q -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/Config.java
git commit -m "feat(core): add DolphinDB partition config fields"
```

---

### Task 6: Wire the 2 props into `ConfigDescriptor.loadProps()` + add DolphinDB data-type exception

**Files:**
- Modify: `core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/ConfigDescriptor.java`

- [ ] **Step 1: Add property loaders after `CNOSDB_SHARD_NUMBER`**

Find:
```java
        config.setCNOSDB_SHARD_NUMBER(
            Integer.parseInt(
                properties.getProperty(
                    "CNOSDB_SHARD_NUMBER", config.getCNOSDB_SHARD_NUMBER() + "")));
```

Add immediately after it:
```java
        config.setDOLPHINDB_PARTITION_DAYS(
            Integer.parseInt(
                properties.getProperty(
                    "DOLPHINDB_PARTITION_DAYS", config.getDOLPHINDB_PARTITION_DAYS() + "")));
        config.setDOLPHINDB_DEVICE_HASH_BUCKETS(
            Integer.parseInt(
                properties.getProperty(
                    "DOLPHINDB_DEVICE_HASH_BUCKETS",
                    config.getDOLPHINDB_DEVICE_HASH_BUCKETS() + "")));
```

- [ ] **Step 2: Add DolphinDB to data-type exception**

Find:
```java
    if (dbType != DBType.IoTDB && dbType != DBType.DoubleIoTDB) {
      // When not iotdb, the last four digits of the data ratio must be 0
      for (int i = config.getTypeNumber() - 4; i < splits.length; i++) {
```

Replace with:
```java
    if (dbType != DBType.IoTDB
        && dbType != DBType.DoubleIoTDB
        && dbType != DBType.DolphinDB) {
      // When not iotdb/dolphindb, the last four digits of the data ratio must be 0
      for (int i = config.getTypeNumber() - 4; i < splits.length; i++) {
```

- [ ] **Step 3: Verify compile**

Run: `mvn compile -pl core -q -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Verify core tests still pass**

Run: `mvn test -pl core -q`
Expected: BUILD SUCCESS, tests pass.

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/cn/edu/tsinghua/iot/benchmark/conf/ConfigDescriptor.java
git commit -m "feat(core): wire DolphinDB props and data-type exception"
```

---

### Task 7: Add DolphinDB section to `config.properties`

**Files:**
- Modify: `configuration/conf/config.properties`

- [ ] **Step 1: Append DolphinDB section after Influxdb 2.x section**

Find:
```properties
############## 被测系统为Influxdb 2.x时扩展参数 ########
# influxdb ORG名
# INFLUXDB_ORG=company1
```

Add immediately after it:
```properties

############## 被测系统为 DolphinDB 时扩展参数 ########
# DFS 一级 RANGE(ts) 分区粒度（按天数），默认 7 天一个分区
# DOLPHINDB_PARTITION_DAYS=7

# DFS 二级 HASH(deviceId) 分区桶数，默认 1000
# 对齐 DolphinDB 官方 IoT 示例 dolphindb/tutorials_cn/iot_examples.md
# DOLPHINDB_DEVICE_HASH_BUCKETS=1000
```

- [ ] **Step 2: Commit**

```bash
git add configuration/conf/config.properties
git commit -m "docs(conf): add DolphinDB extension params to config.properties"
```

---

### Task 8: Wire `DB_DOLPHINDB_3` in `DBFactory`

**Files:**
- Modify: `core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java`

- [ ] **Step 1: Add the case before `DB_MSSQLSERVER`**

Find:
```java
        case DB_QUESTDB:
          dbClass = Constants.QUESTDB_CLASS;
          break;
        case DB_MSSQLSERVER:
```

Replace with:
```java
        case DB_QUESTDB:
          dbClass = Constants.QUESTDB_CLASS;
          break;
        case DB_DOLPHINDB_3:
          dbClass = Constants.DOLPHINDB3_CLASS;
          break;
        case DB_MSSQLSERVER:
```

- [ ] **Step 2: Verify compile**

Run: `mvn compile -pl core -q -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add core/src/main/java/cn/edu/tsinghua/iot/benchmark/tsdb/DBFactory.java
git commit -m "feat(core): wire DB_DOLPHINDB_3 in DBFactory"
```

---

## Phase B: Module scaffolding (Tasks 9–13)

### Task 9: Create `dolphindb-3.0/pom.xml`

**Files:**
- Create: `dolphindb-3.0/pom.xml`

- [ ] **Step 1: Create the file**

Create `dolphindb-3.0/pom.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>cn.edu.tsinghua</groupId>
        <artifactId>iot-benchmark</artifactId>
        <version>1.0.0</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <artifactId>dolphindb-3.0</artifactId>
    <name>Benchmark dolphindb-3.0</name>

    <dependencies>
        <dependency>
            <groupId>cn.edu.tsinghua</groupId>
            <artifactId>core</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.dolphindb</groupId>
            <artifactId>dolphindb-javaapi</artifactId>
            <version>3.00.0.2</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>jcl-over-slf4j</artifactId>
            <version>${org.slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>jul-to-slf4j</artifactId>
            <version>${org.slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${org.slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>${org.slf4j.version}</version>
        </dependency>
    </dependencies>

    <build>
        <finalName>iot-benchmark-dolphindb-3.0</finalName>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.1.0</version>
                <executions>
                    <execution>
                        <id>server-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                        <configuration>
                            <descriptors>
                                <descriptor>src/assembly/assembly.xml</descriptor>
                            </descriptors>
                            <appendAssemblyId>false</appendAssemblyId>
                            <archive>
                                <manifest>
                                    <addDefaultImplementationEntries>true</addDefaultImplementationEntries>
                                    <addDefaultSpecificationEntries>true</addDefaultSpecificationEntries>
                                </manifest>
                            </archive>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Commit**

```bash
git add dolphindb-3.0/pom.xml
git commit -m "feat(dolphindb-3.0): add maven module pom"
```

---

### Task 10: Create `dolphindb-3.0/src/assembly/assembly.xml`

**Files:**
- Create: `dolphindb-3.0/src/assembly/assembly.xml`

- [ ] **Step 1: Create the file**

Create `dolphindb-3.0/src/assembly/assembly.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
<assembly>
    <id>DolphinDB-3.0</id>
    <formats>
        <format>dir</format>
        <format>zip</format>
    </formats>
    <includeBaseDirectory>true</includeBaseDirectory>
    <dependencySets>
        <dependencySet>
            <outputDirectory>lib</outputDirectory>
        </dependencySet>
    </dependencySets>
    <fileSets>
        <fileSet>
            <directory>${maven.multiModuleProjectDirectory}/configuration/bin/</directory>
            <outputDirectory>bin</outputDirectory>
            <fileMode>0755</fileMode>
        </fileSet>
        <fileSet>
            <directory>${maven.multiModuleProjectDirectory}/configuration/conf/</directory>
            <outputDirectory>conf</outputDirectory>
        </fileSet>
    </fileSets>
    <files>
        <file>
            <source>${maven.multiModuleProjectDirectory}/configuration/benchmark.bat</source>
        </file>
        <file>
            <source>${maven.multiModuleProjectDirectory}/configuration/benchmark.sh</source>
            <fileMode>0755</fileMode>
        </file>
        <file>
            <source>${maven.multiModuleProjectDirectory}/configuration/rep-benchmark.sh</source>
            <fileMode>0755</fileMode>
        </file>
        <file>
            <source>${maven.multiModuleProjectDirectory}/configuration/cli-benchmark.sh</source>
            <fileMode>0755</fileMode>
        </file>
        <file>
            <source>${maven.multiModuleProjectDirectory}/configuration/routine</source>
        </file>
        <file>
            <source>${maven.multiModuleProjectDirectory}/LICENSE</source>
        </file>
    </files>
</assembly>
```

- [ ] **Step 2: Commit**

```bash
git add dolphindb-3.0/src/assembly/assembly.xml
git commit -m "feat(dolphindb-3.0): add assembly descriptor"
```

---

### Task 11: Create `dolphindb-3.0/src/main/resources/log4j.properties`

**Files:**
- Create: `dolphindb-3.0/src/main/resources/log4j.properties`

- [ ] **Step 1: Create the file**

Copy the content from `questdb/src/main/resources/log4j.properties` verbatim. Run:

```bash
mkdir -p dolphindb-3.0/src/main/resources
cp questdb/src/main/resources/log4j.properties dolphindb-3.0/src/main/resources/log4j.properties
```

- [ ] **Step 2: Commit**

```bash
git add dolphindb-3.0/src/main/resources/log4j.properties
git commit -m "feat(dolphindb-3.0): add log4j.properties"
```

---

### Task 12: Register module in root `pom.xml`

**Files:**
- Modify: `pom.xml`

- [ ] **Step 1: Add module entry**

Find:
```xml
        <module>questdb</module>
```

Replace with:
```xml
        <module>questdb</module>
        <module>dolphindb-3.0</module>
```

- [ ] **Step 2: Commit**

```bash
git add pom.xml
git commit -m "feat(root): register dolphindb-3.0 module"
```

---

### Task 13: Verify scaffolding builds end-to-end (without DolphinDB.java yet)

This step will FAIL until DolphinDB.java exists. Create a stub now to unblock Phase C.

**Files:**
- Create: `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java`

- [ ] **Step 1: Create stub**

Create file with **stub-only** content (real implementation lands in Phase C):
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.dolphindb3;

import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;

import java.util.List;

public class DolphinDB implements IDatabase {
  public DolphinDB(DBConfig dbConfig) {}

  @Override
  public void init() throws TsdbException {
    throw new TsdbException("DolphinDB adapter not implemented yet");
  }

  @Override
  public void cleanup() throws TsdbException {}

  @Override
  public void close() throws TsdbException {}

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    return 0.0;
  }

  @Override
  public Status insertOneBatch(IBatch batch) throws DBConnectException {
    return new Status(false);
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return new Status(false);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    return new Status(false);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    return new Status(false);
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    return new Status(false);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    return new Status(false);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    return new Status(false);
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return new Status(false);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    return new Status(false);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    return new Status(false);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    return new Status(false);
  }
}
```

- [ ] **Step 2: Build whole project to verify scaffolding**

Run: `mvn clean package -DskipTests -Dspotless.skip=true -q`
Expected: BUILD SUCCESS. The dolphindb-3.0 module compiles, packages, and the assembly produces `dolphindb-3.0/target/iot-benchmark-dolphindb-3.0/`.

- [ ] **Step 3: Apply Spotless to format the stub**

Run: `mvn spotless:apply -pl dolphindb-3.0 -q`
Expected: BUILD SUCCESS, file reformatted to Google Java Format.

- [ ] **Step 4: Commit**

```bash
git add dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java
git commit -m "feat(dolphindb-3.0): add IDatabase stub to unblock module build"
```

---

## Phase C: DolphinDB.java adapter (Tasks 14–24)

### Task 14: Implement skeleton with fields, constructor, `typeMap()`, `init()`, `close()`

**Files:**
- Modify: `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java`

- [ ] **Step 1: Replace the stub with the skeleton**

Replace the **entire content** of `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java` with:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.dolphindb3;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

public class DolphinDB implements IDatabase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(DolphinDB.class);

  private static final String TABLE_NAME = "device_data";

  /** Guard so only one schema client creates the database+table. */
  private static final AtomicBoolean schemaInited = new AtomicBoolean(false);

  /** Guard so only one client drops the database in cleanup. */
  private static final AtomicBoolean cleanupDone = new AtomicBoolean(false);

  /** Schema clients wait on this so all of them return after schema is fully created. */
  private static final CyclicBarrier schemaBarrier =
      new CyclicBarrier(config.getSCHEMA_CLIENT_NUMBER());

  private final DBConfig dbConfig;
  private final String dbPath;

  private Connection jdbcConn;
  private MultithreadedTableWriter mtw;

  public DolphinDB(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
    this.dbPath = "dfs://" + dbConfig.getDB_NAME();
  }

  @Override
  public void init() throws TsdbException {
    try {
      String url =
          String.format(
              "jdbc:dolphindb://%s:%s?user=%s&password=%s",
              dbConfig.getHOST().get(0),
              dbConfig.getPORT().get(0),
              dbConfig.getUSERNAME(),
              dbConfig.getPASSWORD());
      jdbcConn = DriverManager.getConnection(url);
    } catch (SQLException e) {
      LOGGER.error("Failed to connect DolphinDB", e);
      throw new TsdbException("Failed to connect DolphinDB", e);
    }
  }

  @Override
  public void close() throws TsdbException {
    if (mtw != null) {
      try {
        mtw.waitForThreadCompletion();
      } catch (Exception e) {
        LOGGER.warn("Failed to wait for MTW completion", e);
      }
    }
    if (jdbcConn != null) {
      try {
        jdbcConn.close();
      } catch (SQLException e) {
        LOGGER.warn("Failed to close DolphinDB JDBC connection", e);
      }
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    // implemented in Task 16
    throw new TsdbException("cleanup not implemented yet");
  }

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    // implemented in Task 17
    throw new TsdbException("registerSchema not implemented yet");
  }

  @Override
  public Status insertOneBatch(IBatch batch) throws DBConnectException {
    // implemented in Task 19
    return new Status(false);
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return new Status(false);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    return new Status(false);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    return new Status(false);
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    return new Status(false);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    return new Status(false);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    return new Status(false);
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return new Status(false);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    return new Status(false);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    return new Status(false);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    return new Status(false);
  }

  @Override
  public String typeMap(SensorType iotdbSensorType) {
    switch (iotdbSensorType) {
      case BOOLEAN:
        return "BOOL";
      case INT32:
        return "INT";
      case INT64:
        return "LONG";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case TEXT:
      case STRING:
        return "STRING";
      case BLOB:
      case OBJECT:
        return "BLOB";
      case TIMESTAMP:
        return "TIMESTAMP";
      case DATE:
        return "DATE";
      default:
        LOGGER.warn(
            "Unsupported sensorType {}, falling back to STRING.", iotdbSensorType);
        return "STRING";
    }
  }
}
```

- [ ] **Step 2: Compile**

Run: `mvn compile -pl dolphindb-3.0 -am -q`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Apply Spotless**

Run: `mvn spotless:apply -pl dolphindb-3.0 -q`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java
git commit -m "feat(dolphindb-3.0): add skeleton with init/close/typeMap"
```

---

### Task 15: Unit test for `typeMap()`

**Files:**
- Create: `dolphindb-3.0/src/test/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDBTypeMapTest.java`

- [ ] **Step 1: Write the failing test**

Create `dolphindb-3.0/src/test/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDBTypeMapTest.java`:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.dolphindb3;

import cn.edu.tsinghua.iot.benchmark.BenchmarkTestBase;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DolphinDBTypeMapTest extends BenchmarkTestBase {

  private final DolphinDB db = new DolphinDB(new DBConfig());

  @Test
  public void allSensorTypesMapToValidDolphinDBType() {
    assertEquals("BOOL", db.typeMap(SensorType.BOOLEAN));
    assertEquals("INT", db.typeMap(SensorType.INT32));
    assertEquals("LONG", db.typeMap(SensorType.INT64));
    assertEquals("FLOAT", db.typeMap(SensorType.FLOAT));
    assertEquals("DOUBLE", db.typeMap(SensorType.DOUBLE));
    assertEquals("STRING", db.typeMap(SensorType.TEXT));
    assertEquals("STRING", db.typeMap(SensorType.STRING));
    assertEquals("BLOB", db.typeMap(SensorType.BLOB));
    assertEquals("BLOB", db.typeMap(SensorType.OBJECT));
    assertEquals("TIMESTAMP", db.typeMap(SensorType.TIMESTAMP));
    assertEquals("DATE", db.typeMap(SensorType.DATE));
  }
}
```

- [ ] **Step 2: Run the test**

Run: `mvn test -pl dolphindb-3.0 -am -Dtest=DolphinDBTypeMapTest -q`
Expected: BUILD SUCCESS, 1 test, 0 failures.

- [ ] **Step 3: Apply Spotless**

Run: `mvn spotless:apply -pl dolphindb-3.0 -q`

- [ ] **Step 4: Commit**

```bash
git add dolphindb-3.0/src/test/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDBTypeMapTest.java
git commit -m "test(dolphindb-3.0): cover all 11 sensor types in typeMap"
```

---

### Task 16: Implement `cleanup()` — drop DFS database

**Files:**
- Modify: `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java`

- [ ] **Step 1: Replace the cleanup stub**

Find:
```java
  @Override
  public void cleanup() throws TsdbException {
    // implemented in Task 16
    throw new TsdbException("cleanup not implemented yet");
  }
```

Replace with:
```java
  @Override
  public void cleanup() throws TsdbException {
    if (!cleanupDone.compareAndSet(false, true)) {
      return; // another client already dropped the database
    }
    String script = "if(existsDatabase(\"" + dbPath + "\")) { dropDatabase(\"" + dbPath + "\") }";
    try (java.sql.Statement st = jdbcConn.createStatement()) {
      LOGGER.info("Cleanup: {}", script);
      st.execute(script);
    } catch (SQLException e) {
      LOGGER.error("Failed to drop DolphinDB database", e);
      throw new TsdbException("Failed to drop DolphinDB database", e);
    }
  }
```

- [ ] **Step 2: Compile**

Run: `mvn compile -pl dolphindb-3.0 -am -q`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Apply Spotless**

Run: `mvn spotless:apply -pl dolphindb-3.0 -q`

- [ ] **Step 4: Commit**

```bash
git add dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java
git commit -m "feat(dolphindb-3.0): implement cleanup via dropDatabase"
```

---

### Task 17: Implement `registerSchema()` with composite RANGE + HASH partitioning

**Files:**
- Modify: `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java`

- [ ] **Step 1: Add imports**

At the top of the file, add the following imports (after the existing `java.util.List` import):
```java
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.stream.Collectors;
```

- [ ] **Step 2: Replace the registerSchema stub**

Find:
```java
  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    // implemented in Task 17
    throw new TsdbException("registerSchema not implemented yet");
  }
```

Replace with:
```java
  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    long start = System.nanoTime();
    if (config.hasWrite()) {
      try {
        if (schemaInited.compareAndSet(false, true)) {
          createDatabaseAndTable();
        }
        schemaBarrier.await();
      } catch (Exception e) {
        LOGGER.error("Failed to register DolphinDB schema", e);
        throw new TsdbException("Failed to register DolphinDB schema", e);
      }
    }
    return TimeUtils.convertToSeconds(System.nanoTime() - start, "ns");
  }

  private void createDatabaseAndTable() throws SQLException {
    long startMs = TimeUtils.convertDateStrToTimestamp(config.getSTART_TIME());
    long durationMs =
        config.getLOOP() * config.getBATCH_SIZE_PER_WRITE() * config.getPOINT_STEP();
    long endMs = startMs + durationMs;
    long bucketMs = (long) config.getDOLPHINDB_PARTITION_DAYS() * 86_400_000L;
    List<Long> boundaries = new ArrayList<>();
    for (long t = startMs; t < endMs + bucketMs; t += bucketMs) {
      boundaries.add(t);
    }
    String rangeArr =
        boundaries.stream()
            .map(t -> "timestamp(" + t + "l)")
            .collect(Collectors.joining(", ", "[", "]"));

    StringBuilder cols = new StringBuilder("`ts`deviceId");
    StringBuilder types = new StringBuilder("[TIMESTAMP, SYMBOL");
    for (Sensor sensor : config.getSENSORS()) {
      cols.append("`").append(sensor.getName());
      types.append(", ").append(typeMap(sensor.getSensorType()));
    }
    types.append("]");

    String script =
        "rangeBoundaries = "
            + rangeArr
            + "\n"
            + "db1 = database(\"\", RANGE, rangeBoundaries)\n"
            + "db2 = database(\"\", HASH, [SYMBOL, "
            + config.getDOLPHINDB_DEVICE_HASH_BUCKETS()
            + "])\n"
            + "db  = database(\""
            + dbPath
            + "\", COMPO, [db1, db2])\n"
            + "schema = table(1:0, "
            + cols
            + ", "
            + types
            + ")\n"
            + "db.createPartitionedTable(schema, \""
            + TABLE_NAME
            + "\", `ts`deviceId)";
    try (Statement st = jdbcConn.createStatement()) {
      LOGGER.info("Create schema script:\n{}", script);
      st.execute(script);
    }
  }
```

- [ ] **Step 3: Compile**

Run: `mvn compile -pl dolphindb-3.0 -am -q`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Apply Spotless**

Run: `mvn spotless:apply -pl dolphindb-3.0 -q`

- [ ] **Step 5: Commit**

```bash
git add dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java
git commit -m "feat(dolphindb-3.0): implement registerSchema with composite partitioning"
```

---

### Task 18: Implement `ensureMtw()` + `convertValue()` helpers

**Files:**
- Modify: `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java`

- [ ] **Step 1: Add the helpers above the `typeMap` method**

Find:
```java
  @Override
  public String typeMap(SensorType iotdbSensorType) {
```

Insert immediately above it:
```java
  private synchronized void ensureMtw() throws Exception {
    if (mtw != null) return;
    int batchSize = config.getBATCH_SIZE_PER_WRITE() * config.getDEVICE_NUM_PER_WRITE();
    mtw =
        new MultithreadedTableWriter(
            dbConfig.getHOST().get(0),
            Integer.parseInt(dbConfig.getPORT().get(0)),
            dbConfig.getUSERNAME(),
            dbConfig.getPASSWORD(),
            dbPath,
            TABLE_NAME,
            false,
            false,
            null,
            batchSize,
            0.01f,
            1,
            "",
            null,
            MultithreadedTableWriter.Mode.M_Append,
            null);
  }

  private static Object convertValue(Object v, SensorType type) {
    switch (type) {
      case BOOLEAN:
        return (Boolean) v;
      case INT32:
        return (Integer) v;
      case INT64:
        return (Long) v;
      case FLOAT:
        return (Float) v;
      case DOUBLE:
        return (Double) v;
      case TEXT:
      case STRING:
        return String.valueOf(v);
      case BLOB:
      case OBJECT:
        return v instanceof byte[] ? v : String.valueOf(v).getBytes();
      case TIMESTAMP:
        return new java.sql.Timestamp((Long) v);
      case DATE:
        return java.sql.Date.valueOf(String.valueOf(v));
      default:
        return String.valueOf(v);
    }
  }
```

- [ ] **Step 2: Compile**

Run: `mvn compile -pl dolphindb-3.0 -am -q`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Apply Spotless**

Run: `mvn spotless:apply -pl dolphindb-3.0 -q`

- [ ] **Step 4: Commit**

```bash
git add dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java
git commit -m "feat(dolphindb-3.0): add ensureMtw and convertValue helpers"
```

---

### Task 19: Implement `insertOneBatch()` + `awaitDrain()`

**Files:**
- Modify: `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java`

- [ ] **Step 1: Add imports**

At the top of the file, add:
```java
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter.Status.WriteStatus;
import com.xxdb.utils.ErrorCodeInfo;
```

Note: actual class names depend on `com.dolphindb:dolphindb-javaapi:3.00.0.2`. If `WriteStatus` import is wrong, look up the inner class via IDE / `javap`. The `Status` field with `unsentRows` / `errorInfo` is what we read from `mtw.getStatus()`.

- [ ] **Step 2: Replace the insertOneBatch stub**

Find:
```java
  @Override
  public Status insertOneBatch(IBatch batch) throws DBConnectException {
    // implemented in Task 19
    return new Status(false);
  }
```

Replace with:
```java
  @Override
  public Status insertOneBatch(IBatch batch) throws DBConnectException {
    DeviceSchema device = batch.getDeviceSchema();
    String deviceId = device.getDevice();
    List<Sensor> sensors = device.getSensors();
    try {
      ensureMtw();
      for (Record record : batch.getRecords()) {
        Object[] row = new Object[2 + sensors.size()];
        row[0] = new java.sql.Timestamp(record.getTimestamp());
        row[1] = deviceId;
        List<Object> vals = record.getRecordDataValue();
        for (int i = 0; i < sensors.size(); i++) {
          row[i + 2] = convertValue(vals.get(i), sensors.get(i).getSensorType());
        }
        ErrorCodeInfo ret = mtw.insert(row);
        if (ret.errorCode != null && !ret.errorCode.isEmpty()) {
          return new Status(false, 0, new SQLException(ret.errorInfo), ret.errorInfo);
        }
      }
      awaitDrain();
      MultithreadedTableWriter.Status st = mtw.getStatus();
      if (st.errorInfo != null && !st.errorInfo.isEmpty()) {
        return new Status(false, 0, new SQLException(st.errorInfo), st.errorInfo);
      }
      return new Status(true);
    } catch (Exception e) {
      LOGGER.error("Failed to insert batch into DolphinDB", e);
      return new Status(false, 0, e, e.toString());
    }
  }

  private void awaitDrain() throws InterruptedException {
    while (true) {
      MultithreadedTableWriter.Status st = mtw.getStatus();
      if (st.unsentRows == 0) return;
      if (st.errorInfo != null && !st.errorInfo.isEmpty()) return;
      Thread.sleep(1);
    }
  }
```

- [ ] **Step 3: Compile**

Run: `mvn compile -pl dolphindb-3.0 -am -q`
Expected: BUILD SUCCESS. If `ErrorCodeInfo` import fails, run:
```bash
mvn dependency:build-classpath -pl dolphindb-3.0 -q -DincludeArtifactIds=dolphindb-javaapi -Dmdep.outputFile=/tmp/ddb-cp
jar tf $(cat /tmp/ddb-cp) | grep -i ErrorCodeInfo
```
to find the right package; then fix the import.

- [ ] **Step 4: Apply Spotless**

Run: `mvn spotless:apply -pl dolphindb-3.0 -q`

- [ ] **Step 5: Commit**

```bash
git add dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java
git commit -m "feat(dolphindb-3.0): implement insertOneBatch via MTW"
```

---

### Task 20: Add query helper methods

**Files:**
- Modify: `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java`

- [ ] **Step 1: Add imports**

Ensure these imports exist at the top:
```java
import java.sql.ResultSet;
```

- [ ] **Step 2: Add the helper methods above `typeMap`**

Insert immediately above the `typeMap` method:
```java
  private String tableRef() {
    return "loadTable(\"" + dbPath + "\", \"" + TABLE_NAME + "\")";
  }

  private static String tsLiteral(long epochMs) {
    return "timestamp(" + epochMs + "l)";
  }

  private static String deviceInList(List<DeviceSchema> devs) {
    return devs.stream()
        .map(d -> "'" + d.getDevice() + "'")
        .collect(Collectors.joining(", ", "(", ")"));
  }

  private static String sensorColumns(List<Sensor> sensors) {
    return sensors.stream().map(Sensor::getName).collect(Collectors.joining(", "));
  }

  private Status executeQueryAndCount(String sql) {
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), sql);
    }
    int rows = 0;
    try (Statement st = jdbcConn.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      while (rs.next()) rows++;
      long points = (long) rows * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM();
      return new Status(true, points);
    } catch (SQLException e) {
      LOGGER.error("DolphinDB query failed: {}", sql, e);
      return new Status(false, 0, e, e.toString());
    }
  }
```

- [ ] **Step 3: Compile**

Run: `mvn compile -pl dolphindb-3.0 -am -q`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Apply Spotless**

Run: `mvn spotless:apply -pl dolphindb-3.0 -q`

- [ ] **Step 5: Commit**

```bash
git add dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java
git commit -m "feat(dolphindb-3.0): add query SQL builder helpers"
```

---

### Task 21: Implement Q1–Q3 (precise, range, value-range)

**Files:**
- Modify: `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java`

- [ ] **Step 1: Replace the three stubs**

Find:
```java
  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return new Status(false);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    return new Status(false);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    return new Status(false);
  }
```

Replace with:
```java
  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    List<DeviceSchema> devs = preciseQuery.getDeviceSchema();
    String sql =
        "SELECT "
            + sensorColumns(devs.get(0).getSensors())
            + " FROM "
            + tableRef()
            + " WHERE ts = "
            + tsLiteral(preciseQuery.getTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs);
    return executeQueryAndCount(sql);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    List<DeviceSchema> devs = rangeQuery.getDeviceSchema();
    String sql =
        "SELECT "
            + sensorColumns(devs.get(0).getSensors())
            + " FROM "
            + tableRef()
            + " WHERE ts >= "
            + tsLiteral(rangeQuery.getStartTimestamp())
            + " AND ts <= "
            + tsLiteral(rangeQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs);
    return executeQueryAndCount(sql);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    List<DeviceSchema> devs = valueRangeQuery.getDeviceSchema();
    List<Sensor> sensors = devs.get(0).getSensors();
    StringBuilder valueClause = new StringBuilder();
    for (Sensor sensor : sensors) {
      valueClause
          .append(" AND ")
          .append(sensor.getName())
          .append(" > ")
          .append(valueRangeQuery.getValueThreshold());
    }
    String sql =
        "SELECT "
            + sensorColumns(sensors)
            + " FROM "
            + tableRef()
            + " WHERE ts >= "
            + tsLiteral(valueRangeQuery.getStartTimestamp())
            + " AND ts <= "
            + tsLiteral(valueRangeQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs)
            + valueClause;
    return executeQueryAndCount(sql);
  }
```

- [ ] **Step 2: Compile + Spotless + Commit**

Run:
```bash
mvn compile -pl dolphindb-3.0 -am -q
mvn spotless:apply -pl dolphindb-3.0 -q
git add dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java
git commit -m "feat(dolphindb-3.0): implement Q1-Q3 (precise, range, valueRange)"
```

---

### Task 22: Implement Q4–Q6 (agg-range, agg-value, agg-range-value)

**Files:**
- Modify: `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java`

- [ ] **Step 1: Replace the three stubs**

Find:
```java
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    return new Status(false);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    return new Status(false);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    return new Status(false);
  }
```

Replace with:
```java
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    List<DeviceSchema> devs = aggRangeQuery.getDeviceSchema();
    String aggCols =
        devs.get(0).getSensors().stream()
            .map(s -> aggRangeQuery.getAggFun() + "(" + s.getName() + ")")
            .collect(Collectors.joining(", "));
    String sql =
        "SELECT "
            + aggCols
            + " FROM "
            + tableRef()
            + " WHERE ts >= "
            + tsLiteral(aggRangeQuery.getStartTimestamp())
            + " AND ts <= "
            + tsLiteral(aggRangeQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs);
    return executeQueryAndCount(sql);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    List<DeviceSchema> devs = aggValueQuery.getDeviceSchema();
    List<Sensor> sensors = devs.get(0).getSensors();
    String aggCols =
        sensors.stream()
            .map(s -> aggValueQuery.getAggFun() + "(" + s.getName() + ")")
            .collect(Collectors.joining(", "));
    StringBuilder valueClause = new StringBuilder();
    for (int i = 0; i < sensors.size(); i++) {
      valueClause.append(i == 0 ? " WHERE " : " AND ");
      valueClause
          .append(sensors.get(i).getName())
          .append(" > ")
          .append(aggValueQuery.getValueThreshold());
    }
    valueClause.append(" AND deviceId IN ").append(deviceInList(devs));
    String sql = "SELECT " + aggCols + " FROM " + tableRef() + valueClause;
    return executeQueryAndCount(sql);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    List<DeviceSchema> devs = aggRangeValueQuery.getDeviceSchema();
    List<Sensor> sensors = devs.get(0).getSensors();
    String aggCols =
        sensors.stream()
            .map(s -> aggRangeValueQuery.getAggFun() + "(" + s.getName() + ")")
            .collect(Collectors.joining(", "));
    StringBuilder valueClause = new StringBuilder();
    for (Sensor sensor : sensors) {
      valueClause
          .append(" AND ")
          .append(sensor.getName())
          .append(" > ")
          .append(aggRangeValueQuery.getValueThreshold());
    }
    String sql =
        "SELECT "
            + aggCols
            + " FROM "
            + tableRef()
            + " WHERE ts >= "
            + tsLiteral(aggRangeValueQuery.getStartTimestamp())
            + " AND ts <= "
            + tsLiteral(aggRangeValueQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs)
            + valueClause;
    return executeQueryAndCount(sql);
  }
```

- [ ] **Step 2: Compile + Spotless + Commit**

```bash
mvn compile -pl dolphindb-3.0 -am -q
mvn spotless:apply -pl dolphindb-3.0 -q
git add dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java
git commit -m "feat(dolphindb-3.0): implement Q4-Q6 aggregation queries"
```

---

### Task 23: Implement Q7 (group-by) + Q8 (latest-point)

**Files:**
- Modify: `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java`

- [ ] **Step 1: Replace the two stubs**

Find:
```java
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return new Status(false);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    return new Status(false);
  }
```

Replace with:
```java
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    List<DeviceSchema> devs = groupByQuery.getDeviceSchema();
    String aggCols =
        devs.get(0).getSensors().stream()
            .map(s -> groupByQuery.getAggFun() + "(" + s.getName() + ")")
            .collect(Collectors.joining(", "));
    String sql =
        "SELECT "
            + aggCols
            + " FROM "
            + tableRef()
            + " WHERE ts >= "
            + tsLiteral(groupByQuery.getStartTimestamp())
            + " AND ts <= "
            + tsLiteral(groupByQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs)
            + " GROUP BY bar(ts, "
            + groupByQuery.getGranularity()
            + "l)";
    return executeQueryAndCount(sql);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    List<DeviceSchema> devs = latestPointQuery.getDeviceSchema();
    String lastCols =
        devs.get(0).getSensors().stream()
            .map(s -> "last(" + s.getName() + ")")
            .collect(Collectors.joining(", "));
    String sql =
        "SELECT "
            + lastCols
            + " FROM "
            + tableRef()
            + " WHERE deviceId IN "
            + deviceInList(devs);
    return executeQueryAndCount(sql);
  }
```

- [ ] **Step 2: Compile + Spotless + Commit**

```bash
mvn compile -pl dolphindb-3.0 -am -q
mvn spotless:apply -pl dolphindb-3.0 -q
git add dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java
git commit -m "feat(dolphindb-3.0): implement Q7 groupBy and Q8 latestPoint"
```

---

### Task 24: Implement Q9–Q11 (range desc, value-range desc, group-by desc) + Q12 (set-op)

**Files:**
- Modify: `dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java`

- [ ] **Step 1: Add import**

Ensure `SetOpQuery` is imported:
```java
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.SetOpQuery;
```

- [ ] **Step 2: Replace the two desc stubs**

Find:
```java
  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    return new Status(false);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    return new Status(false);
  }
```

Replace with:
```java
  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    List<DeviceSchema> devs = rangeQuery.getDeviceSchema();
    String sql =
        "SELECT "
            + sensorColumns(devs.get(0).getSensors())
            + " FROM "
            + tableRef()
            + " WHERE ts >= "
            + tsLiteral(rangeQuery.getStartTimestamp())
            + " AND ts <= "
            + tsLiteral(rangeQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs)
            + " ORDER BY ts DESC";
    return executeQueryAndCount(sql);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    List<DeviceSchema> devs = valueRangeQuery.getDeviceSchema();
    List<Sensor> sensors = devs.get(0).getSensors();
    StringBuilder valueClause = new StringBuilder();
    for (Sensor sensor : sensors) {
      valueClause
          .append(" AND ")
          .append(sensor.getName())
          .append(" > ")
          .append(valueRangeQuery.getValueThreshold());
    }
    String sql =
        "SELECT "
            + sensorColumns(sensors)
            + " FROM "
            + tableRef()
            + " WHERE ts >= "
            + tsLiteral(valueRangeQuery.getStartTimestamp())
            + " AND ts <= "
            + tsLiteral(valueRangeQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs)
            + valueClause
            + " ORDER BY ts DESC";
    return executeQueryAndCount(sql);
  }

  @Override
  public Status groupByQueryOrderByDesc(GroupByQuery groupByQuery) {
    List<DeviceSchema> devs = groupByQuery.getDeviceSchema();
    String aggCols =
        devs.get(0).getSensors().stream()
            .map(s -> groupByQuery.getAggFun() + "(" + s.getName() + ")")
            .collect(Collectors.joining(", "));
    String sql =
        "SELECT bar(ts, "
            + groupByQuery.getGranularity()
            + "l) AS tb, "
            + aggCols
            + " FROM "
            + tableRef()
            + " WHERE ts >= "
            + tsLiteral(groupByQuery.getStartTimestamp())
            + " AND ts <= "
            + tsLiteral(groupByQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs)
            + " GROUP BY tb ORDER BY tb DESC";
    return executeQueryAndCount(sql);
  }

  @Override
  public Status setOpQuery(SetOpQuery setOpQuery) {
    List<DeviceSchema> devs = setOpQuery.getDeviceSchema();
    String op = config.getQUERY_SET_OP_TYPE().toUpperCase();
    int subQueryNum = config.getQUERY_SET_OP_NUM();
    StringBuilder sql = new StringBuilder();
    long start = setOpQuery.getStartTimestamp();
    long end = setOpQuery.getEndTimestamp();
    long span = (end - start) / Math.max(1, subQueryNum);
    for (int i = 0; i < subQueryNum; i++) {
      if (i > 0) sql.append(" ").append(op).append(" ");
      long s = start + i * span;
      long e = (i == subQueryNum - 1) ? end : (start + (i + 1) * span);
      sql.append("(SELECT ")
          .append(sensorColumns(devs.get(0).getSensors()))
          .append(" FROM ")
          .append(tableRef())
          .append(" WHERE ts >= ")
          .append(tsLiteral(s))
          .append(" AND ts <= ")
          .append(tsLiteral(e))
          .append(" AND deviceId IN ")
          .append(deviceInList(devs))
          .append(")");
    }
    return executeQueryAndCount(sql.toString());
  }
```

- [ ] **Step 3: Compile + Spotless + Commit**

```bash
mvn compile -pl dolphindb-3.0 -am -q
mvn spotless:apply -pl dolphindb-3.0 -q
git add dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java
git commit -m "feat(dolphindb-3.0): implement Q9-Q12 desc and set-op queries"
```

---

## Phase D: Docker installation + E2E (Tasks 25–27)

### Task 25: Start DolphinDB Docker container and verify connectivity

- [ ] **Step 1: Pull image**

Run:
```bash
# Apple Silicon (M1/M2/M3):
docker pull --platform linux/arm64 dolphindb/dolphindb:latest
# Intel Mac:
# docker pull dolphindb/dolphindb:latest
```
Expected: image downloaded.

- [ ] **Step 2: Start container**

Run:
```bash
docker run -itd --name ddb \
  -p 8848:8848 \
  --ulimit nofile=65536:65536 \
  dolphindb/dolphindb:latest \
  sh
```

- [ ] **Step 3: Verify port + login**

Run:
```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" http://127.0.0.1:8848
```
Expected: `HTTP 200`.

- [ ] **Step 4: Smoke-test via simple script through DolphinDB Java API**

Open browser at `http://127.0.0.1:8848` to confirm Web GUI loads. Then login with `admin` / `123456`.

(No commit — this task is operational setup, no code change.)

---

### Task 26: Run write-only smoke test

- [ ] **Step 1: Build the distributable**

From project root:
```bash
mvn clean package -pl dolphindb-3.0 -am -DskipTests -Dspotless.skip=true -q
```
Expected: BUILD SUCCESS. Verify directory exists:
```bash
ls dolphindb-3.0/target/iot-benchmark-dolphindb-3.0/iot-benchmark-dolphindb-3.0/
```
Expected: contains `bin/`, `conf/`, `lib/`, `benchmark.sh`, etc.

- [ ] **Step 2: Edit config for smoke test**

Edit `dolphindb-3.0/target/iot-benchmark-dolphindb-3.0/iot-benchmark-dolphindb-3.0/conf/config.properties`:

Set/uncomment the following keys:
```properties
DB_SWITCH=DolphinDB-3
HOST=127.0.0.1
PORT=8848
USERNAME=admin
PASSWORD=123456
DB_NAME=benchmark
DOLPHINDB_PARTITION_DAYS=7
DOLPHINDB_DEVICE_HASH_BUCKETS=1000

LOOP=100
DEVICE_NUMBER=10
SENSOR_NUMBER=5
BATCH_SIZE_PER_WRITE=100
SCHEMA_CLIENT_NUMBER=2
DATA_CLIENT_NUMBER=2
IS_DELETE_DATA=true
CREATE_SCHEMA=true
INSERT_DATATYPE_PROPORTION=1:1:1:1:1:1:0:0:0:0:0
OPERATION_PROPORTION=1:0:0:0:0:0:0:0:0:0:0:0:0
```

- [ ] **Step 3: Run benchmark**

```bash
cd dolphindb-3.0/target/iot-benchmark-dolphindb-3.0/iot-benchmark-dolphindb-3.0
./benchmark.sh
```
Expected: terminal scrolls write progress; ends with `All dataClients finished` and a Result Matrix where `INGESTION okOperation > 0` and `failOperation = 0`.

- [ ] **Step 4: Verify in DolphinDB**

Open `http://127.0.0.1:8848`, login, run in the web shell:
```dolphindb
pt = loadTable("dfs://benchmark", "device_data")
select count(*) from pt
select count(distinct deviceId) from pt
```
Expected: count = `LOOP * BATCH_SIZE_PER_WRITE * DEVICE_NUMBER / DATA_CLIENT_NUMBER = 100 * 100 * 10 / 2 = 50000`. distinct deviceId = `10`.

- [ ] **Step 5: If anything fails, debug per spec §6.6 and re-run**

Common fixes:
- Wrong port/host → fix `config.properties`
- "database already exists" → set `IS_DELETE_DATA=true`
- MTW error → check `typeMap` + `convertValue` for the offending column

- [ ] **Step 6: Commit any config / code fixes discovered**

```bash
git add -A
git commit -m "fix(dolphindb-3.0): <describe fix from e2e>"
```

(Skip commit if no code changes were needed.)

---

### Task 27: Run mixed read/write E2E (Q1–Q12)

- [ ] **Step 1: Update config to enable all operations**

Edit `dolphindb-3.0/target/iot-benchmark-dolphindb-3.0/iot-benchmark-dolphindb-3.0/conf/config.properties`:
```properties
OPERATION_PROPORTION=1:1:1:1:1:1:1:1:1:1:1:1:1
```

- [ ] **Step 2: Run benchmark**

```bash
./benchmark.sh
```
Expected: Result Matrix has `okOperation > 0` for every Operation row and `failOperation = 0` for every row.

- [ ] **Step 3: Note any query that failed**

If any Q has `failOperation > 0`, capture the SQL from the log, paste into DolphinDB Web Console, and fix the SQL builder in `DolphinDB.java`. Common issues:
- `bar()` granularity unit (ms vs literal long)
- `ORDER BY` alias scoping
- `UNION` requires parentheses around subqueries (already in plan)
- BLOB column in SELECT list — ResultSet may need `getBytes()` not `getString()` (we only count rows so this should be fine)

- [ ] **Step 4: If fixes were needed, commit**

```bash
mvn spotless:apply -pl dolphindb-3.0 -q
git add dolphindb-3.0/src/main/java/cn/edu/tsinghua/iot/benchmark/dolphindb3/DolphinDB.java
git commit -m "fix(dolphindb-3.0): <describe fix>"
```

- [ ] **Step 5: Repeat Steps 2–4 until all 13 operations pass**

---

## Phase E: Documentation (Tasks 28–30)

### Task 28: Update README + README-cn

**Files:**
- Modify: `README.md`, `README-cn.md`

- [ ] **Step 1: Add DolphinDB to "Supported database types" tables**

In `README.md`, find the table:
```markdown
|       TDengine       | 2.2.0.2、3.0.1 |
|      PI Archive      |      2016      |
```

Replace with:
```markdown
|       TDengine       | 2.2.0.2、3.0.1 |
|      DolphinDB       |      v3.x      |
|      PI Archive      |      2016      |
```

In `README-cn.md`, find the corresponding 中文 table and apply the same change (add `|      DolphinDB       |      v3.x      |` row in the same position).

- [ ] **Step 2: Add DolphinDB to DB_SWITCH correspondence tables**

In `README.md`, find:
```markdown
|       TDengine       |  3.0.1   |       tdengine-3.0        |                                                           TDengine-3                                                           |
```

Add immediately after:
```markdown
|       DolphinDB      |   3.x    |       dolphindb-3.0       |                                                          DolphinDB-3                                                           |
```

Apply the same addition to `README-cn.md`.

- [ ] **Step 3: Commit**

```bash
git add README.md README-cn.md
git commit -m "docs: add DolphinDB to supported DB tables"
```

---

### Task 29: Add DolphinDB usage section to DifferentTestDatabase docs

**Files:**
- Modify: `docs/DifferentTestDatabase.md`, `docs/DifferentTestDatabase-cn.md`

- [ ] **Step 1: Append DolphinDB section to `docs/DifferentTestDatabase.md`**

Append at the end of the file:
```markdown

## DolphinDB

DolphinDB v3.x integration. Writes use `MultithreadedTableWriter` (DolphinDB's recommended high-throughput Java API); queries go through the JDBC driver bundled in `dolphindb-javaapi`. Schema is a single DFS partitioned table `device_data` with composite partitioning:

- **Level 1**: `RANGE(ts)` with 7-day granularity by default (`DOLPHINDB_PARTITION_DAYS`)
- **Level 2**: `HASH([SYMBOL, 1000])` on `deviceId` (`DOLPHINDB_DEVICE_HASH_BUCKETS`)

### Start a local DolphinDB via Docker

```bash
# Apple Silicon
docker pull --platform linux/arm64 dolphindb/dolphindb:latest
# Intel
# docker pull dolphindb/dolphindb:latest

docker run -itd --name ddb \
  -p 8848:8848 \
  --ulimit nofile=65536:65536 \
  dolphindb/dolphindb:latest \
  sh
```

Web GUI: `http://127.0.0.1:8848` (default `admin` / `123456`).

### Key config

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
```

- [ ] **Step 2: Append a Chinese equivalent to `docs/DifferentTestDatabase-cn.md`**

Append at the end of `docs/DifferentTestDatabase-cn.md`:
```markdown

## DolphinDB

DolphinDB v3.x 适配。写入使用 `MultithreadedTableWriter`（DolphinDB 官方推荐的高吞吐 Java API），查询走 `dolphindb-javaapi` 内置 JDBC 驱动。建表为单宽表 `device_data`，DFS 复合分区：

- **一级**：`RANGE(ts)`，默认 7 天一个分区（`DOLPHINDB_PARTITION_DAYS`）
- **二级**：`HASH([SYMBOL, 1000])` on `deviceId`（`DOLPHINDB_DEVICE_HASH_BUCKETS`）

### 通过 Docker 启动本地 DolphinDB

```bash
# Apple Silicon
docker pull --platform linux/arm64 dolphindb/dolphindb:latest
# Intel
# docker pull dolphindb/dolphindb:latest

docker run -itd --name ddb \
  -p 8848:8848 \
  --ulimit nofile=65536:65536 \
  dolphindb/dolphindb:latest \
  sh
```

Web GUI: `http://127.0.0.1:8848`（默认 `admin` / `123456`）。

### 关键配置

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

### 注意事项

- DolphinDB 社区版单节点 8GB 内存上限。更大规模 benchmark 需申请企业版授权。
- `DOLPHINDB_DEVICE_HASH_BUCKETS` 默认 1000 对齐 DolphinDB 官方 IoT 示例。设备数极少时可调小（例如 100）以降低元数据开销。
```

- [ ] **Step 3: Commit**

```bash
git add docs/DifferentTestDatabase.md docs/DifferentTestDatabase-cn.md
git commit -m "docs: add DolphinDB usage guide"
```

---

### Task 30: Add DolphinDB-3 row to OperationSupportMatrix docs

**Files:**
- Modify: `docs/OperationSupportMatrix.md`, `docs/OperationSupportMatrix-cn.md`

- [ ] **Step 1: Append DolphinDB-3 row based on E2E results**

In `docs/OperationSupportMatrix.md`, find the existing matrix table (the one with `**TDengine (v2.x)**` row at the bottom). Add a new row using the actual results captured in Task 27:

Template (replace ✅/❌ based on E2E outcome):
```markdown
| **DolphinDB-3** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
```

If any column failed in Task 27 and was not fixable, use ❌ for that column.

Apply the same row addition to `docs/OperationSupportMatrix-cn.md` (Chinese version's matrix is structurally identical).

- [ ] **Step 2: Commit**

```bash
git add docs/OperationSupportMatrix.md docs/OperationSupportMatrix-cn.md
git commit -m "docs: add DolphinDB-3 operation support row"
```

---

## Done criteria

- [ ] All 30 tasks committed
- [ ] `mvn clean package -DskipTests -Dspotless.skip=true` succeeds
- [ ] `mvn test -pl core,dolphindb-3.0` passes (typeMap test + existing core tests)
- [ ] `mvn spotless:check` passes (no formatting drift)
- [ ] Smoke test (Task 26): `INGESTION okOperation > 0`, `failOperation = 0`, `count(*)` and `count(distinct deviceId)` match expectations
- [ ] Mixed test (Task 27): all 13 operations succeed; if any failed, root cause documented in OperationSupportMatrix row
- [ ] All 6 documentation files updated (README × 2, DifferentTestDatabase × 2, OperationSupportMatrix × 2)
