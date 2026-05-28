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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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

  private DBConnection conn;
  private MultithreadedTableWriter mtw;

  public DolphinDB(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
    this.dbPath = "dfs://" + dbConfig.getDB_NAME();
  }

  @Override
  public void init() throws TsdbException {
    try {
      conn = new DBConnection();
      boolean ok =
          conn.connect(
              dbConfig.getHOST().get(0),
              Integer.parseInt(dbConfig.getPORT().get(0)),
              dbConfig.getUSERNAME(),
              dbConfig.getPASSWORD());
      if (!ok) {
        throw new TsdbException("Failed to connect DolphinDB: connect() returned false");
      }
    } catch (IOException e) {
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
    if (conn != null) {
      conn.close();
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    if (!cleanupDone.compareAndSet(false, true)) {
      return; // another client already dropped the database
    }
    String script = "if(existsDatabase(\"" + dbPath + "\")) { dropDatabase(\"" + dbPath + "\") }";
    try {
      LOGGER.info("Cleanup: {}", script);
      conn.run(script);
    } catch (IOException e) {
      LOGGER.error("Failed to drop DolphinDB database", e);
      throw new TsdbException("Failed to drop DolphinDB database", e);
    }
  }

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

  private void createDatabaseAndTable() throws IOException {
    long startMs = TimeUtils.convertDateStrToTimestamp(config.getSTART_TIME());
    long durationMs = config.getLOOP() * config.getBATCH_SIZE_PER_WRITE() * config.getPOINT_STEP();
    long endMs = startMs + durationMs;
    long bucketMs = (long) config.getDOLPHINDB_PARTITION_DAYS() * 86_400_000L;
    // DolphinDB RANGE partition boundaries must be DATE literals (YYYY.MM.DD), not TIMESTAMP.
    // A TIMESTAMP column can be partitioned using DATE-type range boundaries.
    List<Long> boundaries = new ArrayList<>();
    for (long t = startMs; t < endMs + bucketMs; t += bucketMs) {
      boundaries.add(t);
    }
    String rangeArr =
        boundaries.stream()
            .map(t -> epochMsToDateLiteral(t))
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
    LOGGER.info("Create schema script:\n{}", script);
    conn.run(script);
  }

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
        if (ret.hasError()) {
          return new Status(false, 0, new Exception(ret.getErrorInfo()), ret.getErrorInfo());
        }
      }
      awaitDrain();
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

  private void awaitDrain() throws InterruptedException {
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
        "SELECT " + lastCols + " FROM " + tableRef() + " WHERE deviceId IN " + deviceInList(devs);
    return executeQueryAndCount(sql);
  }

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
          .append(tableRef())
          .append(" WHERE ts >= ")
          .append(tsLiteral(child.getStartTimestamp()))
          .append(" AND ts <= ")
          .append(tsLiteral(child.getEndTimestamp()))
          .append(" AND deviceId IN ")
          .append(deviceInList(devs))
          .append(")");
    }
    return executeQueryAndCount(sql.toString());
  }

  private synchronized void ensureMtw() throws Exception {
    if (mtw != null) return;
    int batchSize = config.getBATCH_SIZE_PER_WRITE() * config.getDEVICE_NUM_PER_WRITE();
    // "ts" is the first (RANGE) partition column and is required by MTW for partitioned tables.
    // For COMPO partitions, either partitioning column name is accepted.
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
            "ts",
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

  /**
   * Converts an epoch millisecond timestamp to a DolphinDB DATE literal string (YYYY.MM.DD).
   * DolphinDB RANGE partition boundaries must be DATE literals; a TIMESTAMP column can be range-
   * partitioned using DATE-type boundaries (DolphinDB handles the implicit widening).
   */
  private static String epochMsToDateLiteral(long epochMs) {
    java.time.LocalDate date =
        java.time.Instant.ofEpochMilli(epochMs).atZone(java.time.ZoneOffset.UTC).toLocalDate();
    return String.format(
        "%04d.%02d.%02d", date.getYear(), date.getMonthValue(), date.getDayOfMonth());
  }

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
