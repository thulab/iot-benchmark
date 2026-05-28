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
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.ColumnCategory;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.SetOpQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import com.xxdb.DBConnection;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.Entity;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DolphinDB implements IDatabase {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(DolphinDB.class);

  /** Guard so only one schema client creates the databases+tables. */
  private static final AtomicBoolean schemaInited = new AtomicBoolean(false);

  /** Guard so only one client drops the databases in cleanup. */
  private static final AtomicBoolean cleanupDone = new AtomicBoolean(false);

  /** Schema clients wait on this so all of them return after schema is fully created. */
  private static final CyclicBarrier schemaBarrier =
      new CyclicBarrier(config.getSCHEMA_CLIENT_NUMBER());

  /** All DFS database paths created during registerSchema, used in cleanup. */
  private static final Set<String> createdDbPaths = ConcurrentHashMap.newKeySet();

  private final DBConfig dbConfig;
  private final String timeColumn = config.getTABLE_TIME_COLUMN();

  private DBConnection conn;

  /** Per-instance MTW cache keyed by "dbPath::tableName". */
  private final Map<String, MultithreadedTableWriter> mtwCache = new HashMap<>();

  public DolphinDB(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  @Override
  public void init() throws TsdbException {
    // RSA-based login can race on key files when many clients connect concurrently; retry.
    IOException lastError = null;
    for (int attempt = 1; attempt <= 3; attempt++) {
      try {
        conn = new DBConnection();
        boolean ok =
            conn.connect(
                dbConfig.getHOST().get(0),
                Integer.parseInt(dbConfig.getPORT().get(0)),
                dbConfig.getUSERNAME(),
                dbConfig.getPASSWORD());
        if (ok) {
          return;
        }
        throw new IOException("connect() returned false");
      } catch (IOException e) {
        lastError = e;
        LOGGER.warn("DolphinDB connect attempt {}/3 failed: {}", attempt, e.getMessage());
        if (conn != null) {
          try {
            conn.close();
          } catch (Exception ignored) {
            // best-effort cleanup
          }
          conn = null;
        }
        if (attempt < 3) {
          try {
            Thread.sleep(200L * attempt);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TsdbException("Interrupted while connecting to DolphinDB", ie);
          }
        }
      }
    }
    LOGGER.error("Failed to connect DolphinDB after 3 attempts", lastError);
    throw new TsdbException("Failed to connect DolphinDB after 3 attempts", lastError);
  }

  @Override
  public void close() throws TsdbException {
    for (MultithreadedTableWriter mtw : mtwCache.values()) {
      try {
        mtw.waitForThreadCompletion();
      } catch (Exception e) {
        LOGGER.warn("Failed to wait for MTW completion", e);
      }
    }
    mtwCache.clear();
    if (conn != null) {
      conn.close();
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    if (!cleanupDone.compareAndSet(false, true)) {
      return; // another client already dropped the databases
    }
    for (String dbPath : createdDbPaths) {
      String script = "if(existsDatabase(\"" + dbPath + "\")) { dropDatabase(\"" + dbPath + "\") }";
      try {
        LOGGER.info("Cleanup: {}", script);
        conn.run(script);
      } catch (IOException e) {
        LOGGER.error("Failed to drop DolphinDB database {}", dbPath, e);
        throw new TsdbException("Failed to drop DolphinDB database " + dbPath, e);
      }
    }
  }

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    long start = System.nanoTime();
    if (config.hasWrite()) {
      try {
        if (schemaInited.compareAndSet(false, true)) {
          createDatabasesAndTables(schemaList);
        }
        schemaBarrier.await();
      } catch (Exception e) {
        LOGGER.error("Failed to register DolphinDB schema", e);
        throw new TsdbException("Failed to register DolphinDB schema", e);
      }
    }
    return TimeUtils.convertToSeconds(System.nanoTime() - start, "ns");
  }

  private void createDatabasesAndTables(List<DeviceSchema> schemaList) throws IOException {
    Map<String, Set<String>> groupToTables = new LinkedHashMap<>();
    for (DeviceSchema schema : schemaList) {
      groupToTables
          .computeIfAbsent(schema.getGroup(), k -> new LinkedHashSet<>())
          .add(schema.getTable());
    }

    long startMs = TimeUtils.convertDateStrToTimestamp(config.getSTART_TIME());
    long durationMs =
        (long) config.getLOOP() * config.getBATCH_SIZE_PER_WRITE() * config.getPOINT_STEP();
    long endMs = startMs + durationMs + 86_400_000L; // pad by 1 day for the right boundary
    String startDate = epochMsToDateLiteral(startMs);
    String endDate = epochMsToDateLiteral(endMs);

    for (Map.Entry<String, Set<String>> entry : groupToTables.entrySet()) {
      String dbPath = "dfs://" + dbConfig.getDB_NAME() + "_" + entry.getKey();
      createdDbPaths.add(dbPath);

      // Drop pre-existing database so reruns are idempotent.
      conn.run("if(existsDatabase(\"" + dbPath + "\")) { dropDatabase(\"" + dbPath + "\") }");

      String createDbSql =
          "create database \""
              + dbPath
              + "\" partitioned by VALUE("
              + startDate
              + ".."
              + endDate
              + "), HASH([SYMBOL, "
              + config.getDOLPHINDB_DEVICE_HASH_BUCKETS()
              + "]), engine='TSDB', atomic='CHUNK'";
      LOGGER.info("Create DB: {}", createDbSql);
      conn.run(createDbSql);

      for (String tableName : entry.getValue()) {
        String createTableSql = buildCreateTableSql(dbPath, tableName);
        LOGGER.info("Create Table: {}", createTableSql);
        conn.run(createTableSql);
      }
    }
  }

  private String buildCreateTableSql(String dbPath, String tableName) {
    StringBuilder cols = new StringBuilder();
    cols.append(timeColumn).append(" TIMESTAMP,\n");
    cols.append("    deviceId SYMBOL");
    int tagCount = config.getTAG_NUMBER();
    for (int t = 0; t < tagCount; t++) {
      cols.append(",\n    ").append(config.getTAG_KEY_PREFIX()).append(t).append(" SYMBOL");
    }
    for (Sensor sensor : config.getSENSORS()) {
      cols.append(",\n    ").append(sensor.getName()).append(" ");
      // ColumnCategory.TAG sensors are categorical; force SYMBOL regardless of declared SensorType.
      cols.append(
          sensor.getColumnCategory() == ColumnCategory.TAG
              ? "SYMBOL"
              : typeMap(sensor.getSensorType()));
    }

    StringBuilder sortCols = new StringBuilder("[`deviceId");
    for (int t = 0; t < tagCount; t++) {
      sortCols.append(", `").append(config.getTAG_KEY_PREFIX()).append(t);
    }
    sortCols.append(", `").append(timeColumn).append("]");

    // partitioned-by order must match db: VALUE(<time>) then HASH(deviceId).
    return "create table \""
        + dbPath
        + "\".\""
        + tableName
        + "\" (\n    "
        + cols
        + "\n)\npartitioned by "
        + timeColumn
        + ", deviceId,\n"
        + "sortColumns="
        + sortCols
        + ",\n"
        + "keepDuplicates=LAST";
  }

  @Override
  public Status insertOneBatch(IBatch batch) throws DBConnectException {
    int tagCount = config.getTAG_NUMBER();
    try {
      batch.reset();
      // Every device in a MultiDeviceBatch is guaranteed to belong to the same table (the framework
      // partitions devices per client per table), so the first device picks the writer for the
      // whole batch.
      DeviceSchema first = batch.getDeviceSchema();
      List<Sensor> sensors = first.getSensors();
      MultithreadedTableWriter mtw = ensureMtw(dbPathOf(first), first.getTable());
      while (true) {
        DeviceSchema device = batch.getDeviceSchema();
        String deviceId = device.getDevice();
        Object[] tagValues = new Object[tagCount];
        for (int t = 0; t < tagCount; t++) {
          tagValues[t] = device.getTags().get(config.getTAG_KEY_PREFIX() + t);
        }
        for (Record record : batch.getRecords()) {
          Object[] row = new Object[2 + tagCount + sensors.size()];
          row[0] = new java.sql.Timestamp(record.getTimestamp());
          row[1] = deviceId;
          for (int t = 0; t < tagCount; t++) {
            row[2 + t] = tagValues[t];
          }
          List<Object> vals = record.getRecordDataValue();
          for (int i = 0; i < sensors.size(); i++) {
            row[2 + tagCount + i] = convertValue(vals.get(i), sensors.get(i).getSensorType());
          }
          ErrorCodeInfo ret = mtw.insert(row);
          if (ret.hasError()) {
            return new Status(false, 0, new Exception(ret.getErrorInfo()), ret.getErrorInfo());
          }
        }
        if (!batch.hasNext()) break;
        batch.next();
      }
      awaitDrain(mtw);
      MultithreadedTableWriter.Status st = mtw.getStatus();
      if (st.hasError()) {
        return new Status(false, 0, new Exception(st.getErrorInfo()), st.getErrorInfo());
      }
      return new Status(true);
    } catch (Exception e) {
      LOGGER.error("Failed to insert batch into DolphinDB", e);
      return new Status(false, 0, e, e.toString());
    }
  }

  private void awaitDrain(MultithreadedTableWriter mtw) throws InterruptedException {
    while (true) {
      MultithreadedTableWriter.Status st = mtw.getStatus();
      if (st.unsentRows == 0) return;
      if (st.hasError()) return;
      Thread.sleep(1);
    }
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    List<DeviceSchema> devs = preciseQuery.getDeviceSchema();
    String sql =
        "SELECT "
            + sensorColumns(devs.get(0).getSensors())
            + " FROM "
            + tableRef(devs.get(0))
            + " WHERE "
            + timeColumn
            + " = "
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
            + tableRef(devs.get(0))
            + " WHERE "
            + timeColumn
            + " >= "
            + tsLiteral(rangeQuery.getStartTimestamp())
            + " AND "
            + timeColumn
            + " <= "
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
            + tableRef(devs.get(0))
            + " WHERE "
            + timeColumn
            + " >= "
            + tsLiteral(valueRangeQuery.getStartTimestamp())
            + " AND "
            + timeColumn
            + " <= "
            + tsLiteral(valueRangeQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs)
            + valueClause;
    return executeQueryAndCount(sql);
  }

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
            + tableRef(devs.get(0))
            + " WHERE "
            + timeColumn
            + " >= "
            + tsLiteral(aggRangeQuery.getStartTimestamp())
            + " AND "
            + timeColumn
            + " <= "
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
    String sql = "SELECT " + aggCols + " FROM " + tableRef(devs.get(0)) + valueClause;
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
            + tableRef(devs.get(0))
            + " WHERE "
            + timeColumn
            + " >= "
            + tsLiteral(aggRangeValueQuery.getStartTimestamp())
            + " AND "
            + timeColumn
            + " <= "
            + tsLiteral(aggRangeValueQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs)
            + valueClause;
    return executeQueryAndCount(sql);
  }

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
            + tableRef(devs.get(0))
            + " WHERE "
            + timeColumn
            + " >= "
            + tsLiteral(groupByQuery.getStartTimestamp())
            + " AND "
            + timeColumn
            + " <= "
            + tsLiteral(groupByQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs)
            + " GROUP BY bar("
            + timeColumn
            + ", "
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
            + tableRef(devs.get(0))
            + " WHERE deviceId IN "
            + deviceInList(devs);
    return executeQueryAndCount(sql);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    List<DeviceSchema> devs = rangeQuery.getDeviceSchema();
    String sql =
        "SELECT "
            + sensorColumns(devs.get(0).getSensors())
            + " FROM "
            + tableRef(devs.get(0))
            + " WHERE "
            + timeColumn
            + " >= "
            + tsLiteral(rangeQuery.getStartTimestamp())
            + " AND "
            + timeColumn
            + " <= "
            + tsLiteral(rangeQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs)
            + " ORDER BY "
            + timeColumn
            + " DESC";
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
            + tableRef(devs.get(0))
            + " WHERE "
            + timeColumn
            + " >= "
            + tsLiteral(valueRangeQuery.getStartTimestamp())
            + " AND "
            + timeColumn
            + " <= "
            + tsLiteral(valueRangeQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs)
            + valueClause
            + " ORDER BY "
            + timeColumn
            + " DESC";
    return executeQueryAndCount(sql);
  }

  @Override
  public Status groupByQueryOrderByDesc(GroupByQuery groupByQuery) {
    List<DeviceSchema> devs = groupByQuery.getDeviceSchema();
    String aggCols =
        devs.get(0).getSensors().stream()
            .map(s -> groupByQuery.getAggFun() + "(" + s.getName() + ")")
            .collect(Collectors.joining(", "));
    // Distributed SQL forbids mixing raw expressions and aggregates in SELECT;
    // alias the bar() bucket in GROUP BY and reference the alias in ORDER BY.
    String barExpr = "bar(" + timeColumn + ", " + groupByQuery.getGranularity() + "l)";
    String sql =
        "SELECT "
            + aggCols
            + " FROM "
            + tableRef(devs.get(0))
            + " WHERE "
            + timeColumn
            + " >= "
            + tsLiteral(groupByQuery.getStartTimestamp())
            + " AND "
            + timeColumn
            + " <= "
            + tsLiteral(groupByQuery.getEndTimestamp())
            + " AND deviceId IN "
            + deviceInList(devs)
            + " GROUP BY "
            + barExpr
            + " AS bucket"
            + " ORDER BY bucket DESC";
    return executeQueryAndCount(sql);
  }

  @Override
  public Status setOpQuery(SetOpQuery setOpQuery) {
    List<RangeQuery> childQueries = setOpQuery.getChildRangeQueries();
    String op = setOpQuery.getSetOpType().toUpperCase();
    StringBuilder sql = new StringBuilder();
    for (int i = 0; i < childQueries.size(); i++) {
      if (i > 0) sql.append(" ").append(op).append(" ");
      RangeQuery child = childQueries.get(i);
      List<DeviceSchema> devs = child.getDeviceSchema();
      sql.append("(SELECT ")
          .append(sensorColumns(devs.get(0).getSensors()))
          .append(" FROM ")
          .append(tableRef(devs.get(0)))
          .append(" WHERE ")
          .append(timeColumn)
          .append(" >= ")
          .append(tsLiteral(child.getStartTimestamp()))
          .append(" AND ")
          .append(timeColumn)
          .append(" <= ")
          .append(tsLiteral(child.getEndTimestamp()))
          .append(" AND deviceId IN ")
          .append(deviceInList(devs))
          .append(")");
    }
    return executeQueryAndCount(sql.toString());
  }

  private synchronized MultithreadedTableWriter ensureMtw(String dbPath, String tableName)
      throws Exception {
    String key = dbPath + "::" + tableName;
    MultithreadedTableWriter mtw = mtwCache.get(key);
    if (mtw != null) return mtw;
    int batchSize = config.getBATCH_SIZE_PER_WRITE() * config.getDEVICE_NUM_PER_WRITE();
    // partitionCol is the time column (the VALUE-partitioned column at the DB level).
    mtw =
        new MultithreadedTableWriter(
            dbConfig.getHOST().get(0),
            Integer.parseInt(dbConfig.getPORT().get(0)),
            dbConfig.getUSERNAME(),
            dbConfig.getPASSWORD(),
            dbPath,
            tableName,
            false,
            false,
            null,
            batchSize,
            0.01f,
            1,
            timeColumn,
            null,
            MultithreadedTableWriter.Mode.M_Append,
            null);
    mtwCache.put(key, mtw);
    return mtw;
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

  // VALUE() partition boundaries require DolphinDB DATE literal (YYYY.MM.DD).
  private static String epochMsToDateLiteral(long epochMs) {
    java.time.LocalDate date =
        java.time.Instant.ofEpochMilli(epochMs).atZone(java.time.ZoneOffset.UTC).toLocalDate();
    return String.format(
        "%04d.%02d.%02d", date.getYear(), date.getMonthValue(), date.getDayOfMonth());
  }

  private String dbPathOf(DeviceSchema schema) {
    return "dfs://" + dbConfig.getDB_NAME() + "_" + schema.getGroup();
  }

  private String tableRef(DeviceSchema schema) {
    return "loadTable(\"" + dbPathOf(schema) + "\", \"" + schema.getTable() + "\")";
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
    try {
      Entity result = conn.run(sql);
      int rows = result.rows();
      long points = (long) rows * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM();
      return new Status(true, points);
    } catch (IOException e) {
      LOGGER.error("DolphinDB query failed: {}", sql, e);
      return new Status(false, 0, e, e.toString());
    }
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
        LOGGER.warn("Unsupported sensorType {}, falling back to STRING.", iotdbSensorType);
        return "STRING";
    }
  }
}
