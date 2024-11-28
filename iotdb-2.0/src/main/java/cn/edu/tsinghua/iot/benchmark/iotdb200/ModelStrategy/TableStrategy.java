/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.iotdb200.ModelStrategy;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.iotdb200.IoTDB;
import cn.edu.tsinghua.iot.benchmark.iotdb200.TimeseriesSchema;
import cn.edu.tsinghua.iot.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableStrategy extends IoTDBModelStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableStrategy.class);
  protected static final Set<String> tables = new HashSet<>();

  public TableStrategy(DBConfig dbConfig) {
    super(dbConfig);
    ROOT_SERIES_NAME = IoTDB.ROOT_SERIES_NAME;
    queryBaseOffset = 1;
  }

  @Override
  public void registerSchema(
      Map<Session, List<TimeseriesSchema>> sessionListMap, List<DeviceSchema> schemaList)
      throws TsdbException {
    try {
      for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
        registerDatabases(pair.getKey(), pair.getValue());
      }
      schemaBarrier.await();
      for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
        registerTable(pair.getKey(), pair.getValue());
      }
    } catch (Exception e) {
      throw new TsdbException(e);
    }
  }

  /** root.test.g_0.d_0 test is the database name.Ensure that only one client creates the table. */
  @Override
  public void registerDatabases(Session metaSession, List<TimeseriesSchema> schemaList)
      throws TsdbException {
    Set<String> databaseNames = getAllDataBase(schemaList);
    // register storage groups
    for (String databaseName : databaseNames) {
      try {
        metaSession.executeNonQueryStatement(
            "CREATE DATABASE " + dbConfig.getDB_NAME() + "_" + databaseName);
      } catch (Exception e) {
        handleRegisterException(e);
      }
    }
  }

  private void registerTable(Session metaSession, List<TimeseriesSchema> timeseriesSchemas)
      throws TsdbException {
    try {
      // get all tables
      Set<String> tableNames = getAllTables(timeseriesSchemas);
      // register tables
      DeviceSchema deviceSchema = timeseriesSchemas.get(0).getDeviceSchema();
      HashMap<String, List<String>> tables = new HashMap<>();
      for (String tableName : tableNames) {
        StringBuilder builder = new StringBuilder();
        builder.append("create table if not exists ").append(tableName).append("(");
        for (int i = 0; i < deviceSchema.getSensors().size(); i++) {
          if (i != 0) builder.append(", ");
          builder
              .append(deviceSchema.getSensors().get(i).getName())
              .append(" ")
              .append(deviceSchema.getSensors().get(i).getSensorType())
              .append(" ")
              .append(deviceSchema.getSensors().get(i).getColumnCategory());
        }
        builder
            .append(", device_id")
            .append(" ")
            .append(SensorType.STRING)
            .append(" ")
            .append(Tablet.ColumnType.ID);
        for (String key : deviceSchema.getTags().keySet()) {
          builder
              .append(", ")
              .append(key)
              .append(" ")
              .append(SensorType.STRING)
              .append(" ")
              .append(Tablet.ColumnType.ID);
        }
        builder.append(")");
        tables
            .computeIfAbsent(
                dbConfig.getDB_NAME() + "_" + deviceSchema.getGroup(), k -> new ArrayList<>())
            .add(builder.toString());
      }

      for (Map.Entry<String, List<String>> database : tables.entrySet()) {
        metaSession.executeNonQueryStatement("use " + database.getKey());
        for (String table : database.getValue()) {
          metaSession.executeNonQueryStatement(table);
        }
      }
    } catch (Exception e) {
      handleRegisterException(e);
    }
  }

  public Set<String> getAllTables(List<TimeseriesSchema> schemaList) {
    Set<String> tableNames = new HashSet<>();
    for (TimeseriesSchema timeseriesSchema : schemaList) {
      DeviceSchema schema = timeseriesSchema.getDeviceSchema();
      synchronized (IoTDB.class) {
        if (!tables.contains(schema.getTable())) {
          tableNames.add(schema.getTable());
          tables.add(schema.getTable());
        }
      }
    }
    return tableNames;
  }

  // region select

  @Override
  public String selectTimeColumnIfNecessary() {
    if (config.getBENCHMARK_WORK_MODE() == BenchmarkMode.VERIFICATION_QUERY) {
      return "time, ";
    } else {
      return "";
    }
  }

  @Override
  public void addFromClause(List<DeviceSchema> devices, StringBuilder builder) {
    // "root.test.g_0.d_0.s_0"
    // tree mode: select ... from root.test.g_0.d_0
    // table mode： select ... from test_g_0.table_0
    builder
        .append(" FROM ")
        .append(dbConfig.getDB_NAME())
        .append("_")
        .append(devices.get(0).getGroup())
        .append(".")
        .append(devices.get(0).getTable());
  }

  @Override
  public void addOrderByTimeDesc(StringBuilder builder) {
    builder.append(" ORDER BY device_id, time desc");
  }

  @Override
  public String getAggQuerySqlHead(List<DeviceSchema> devices, String aggFun) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT device_id");
    List<Sensor> querySensors = devices.get(0).getSensors();
    String timeArg = getTimeArg(aggFun);
    for (int i = 0; i < querySensors.size(); i++) {
      // The builder has already concatenated "SELECT device_id", so each subsequent loop needs to
      // concatenate one", ".
      builder.append(", ");
      builder
          .append(aggFun)
          .append("(")
          .append(timeArg)
          .append(querySensors.get(i).getName())
          .append(")");
    }
    addFromClause(devices, builder);
    return builder.toString();
  }

  /**
   * eg. SELECT device_id, data_bin(20000ms, time), count(s_3), count(s_2), count(s_1) FROM
   * test_g_1.table_4 WHERE time >= 1640966400000 AND time < 1640966650000 AND (device_id = 'd_0' OR
   * device_id = 'd_1') group by device_id, data_bin(20000ms, time)
   *
   * <p>getAggForGroupByQuery
   */
  @Override
  public String getGroupByQuerySQL(GroupByQuery groupByQuery, boolean addOrderBy) {
    StringBuilder builder = new StringBuilder();
    // SELECT
    builder
        .append("SELECT")
        .append(" device_id,")
        .append(" date_bin(")
        .append(groupByQuery.getGranularity())
        .append("ms, ")
        .append("time), ")
        .append(
            getAggFunForGroupByQuery(
                groupByQuery.getDeviceSchema().get(0).getSensors(), groupByQuery.getAggFun()));
    // FROM
    addFromClause(groupByQuery.getDeviceSchema(), builder);
    // WHERE
    builder
        .append(" WHERE")
        .append(
            getTimeWhereClause(groupByQuery.getStartTimestamp(), groupByQuery.getEndTimestamp()));
    addDeviceIDColumnIfNecessary(groupByQuery.getDeviceSchema(), builder);
    // GROUP BY
    builder
        .append(" group by device_id, date_bin(")
        .append(groupByQuery.getGranularity())
        .append("ms, time)");
    // ORDER BY
    if (addOrderBy) {
      builder
          .append(" order by device_id, date_bin(")
          .append(groupByQuery.getGranularity())
          .append("ms, time) desc");
    }
    return builder.toString();
  }

  private void addDeviceIDColumnIfNecessary(
      List<DeviceSchema> deviceSchemas, StringBuilder builder) {
    builder.append(" AND").append(getDeviceIDColumn(deviceSchemas));
  }

  @Override
  public void addPreciseQueryWhereClause(
      String strTime, List<DeviceSchema> deviceSchemas, StringBuilder builder) {
    builder
        .append(" WHERE time = ")
        .append(strTime)
        .append(" AND ")
        .append(getDeviceIDColumn(deviceSchemas));
  }

  @Override
  public void addWhereClause(
      boolean addTime,
      boolean addValue,
      long start,
      long end,
      List<DeviceSchema> deviceSchemas,
      int valueThreshold,
      StringBuilder builder) {
    builder.append(" WHERE");
    if (addTime) {
      builder.append(getTimeWhereClause(start, end));
    }
    if (addValue) {
      builder.append(getValueFilterClause(deviceSchemas, valueThreshold));
    }
    builder.append(" AND ").append(getDeviceIDColumn(deviceSchemas));
  }

  @Override
  public void addAggWhereClause(
      boolean addTime,
      boolean addValue,
      long start,
      long end,
      List<DeviceSchema> deviceSchemas,
      int valueThreshold,
      StringBuilder builder) {
    builder.append(" WHERE");
    if (addTime) {
      builder.append(getTimeWhereClause(start, end));
    }
    if (addValue) {
      String valueFilterClause = getValueFilterClause(deviceSchemas, valueThreshold);
      if (!addTime) {
        valueFilterClause = valueFilterClause.substring(4);
      }
      builder.append(valueFilterClause);
    }
    builder.append(" AND ").append(getDeviceIDColumn(deviceSchemas)).append(" GROUP BY device_id ");
  }

  @Override
  public String addGroupByClauseIfNecessary(String sql) {
    sql = sql + " GROUP BY device_id";
    return sql;
  }

  @Override
  public String getLatestPointQuerySql(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT device_id, last(time)");
    List<Sensor> querySensors = devices.get(0).getSensors();
    for (int i = 0; i < querySensors.size(); i++) {
      builder.append(", last_by(").append(querySensors.get(i).getName()).append(", time)");
    }
    addFromClause(devices, builder);
    addWhereValueClauseIfNecessary(devices, builder);
    return builder.toString();
  }

  public String getDeviceIDColumn(List<DeviceSchema> deviceSchemas) {
    Set<String> deviceIds = new HashSet<>();
    StringBuilder builder = new StringBuilder();
    builder
        .append(" (")
        .append("device_id")
        .append(" = '")
        .append(deviceSchemas.get(0).getDevice())
        .append("'");
    for (int i = 1; i < deviceSchemas.size(); i++) {
      if (!deviceIds.contains(deviceSchemas.get(i).getDevice())) {
        deviceIds.add(deviceSchemas.get(i).getDevice());
        builder
            .append(" OR ")
            .append("device_id")
            .append(" = '")
            .append(deviceSchemas.get(i).getDevice())
            .append("'");
      }
    }
    builder.append(")");
    return builder.toString();
  }

  @Override
  public void addVerificationQueryWhereClause(
      StringBuffer sql,
      List<Record> records,
      Map<Long, List<Object>> recordMap,
      DeviceSchema deviceSchema) {
    sql.append(" WHERE (time = ").append(records.get(0).getTimestamp());
    recordMap.put(records.get(0).getTimestamp(), records.get(0).getRecordDataValue());
    for (int i = 1; i < records.size(); i++) {
      Record record = records.get(i);
      sql.append(" or time = ").append(record.getTimestamp());
      recordMap.put(record.getTimestamp(), record.getRecordDataValue());
    }
    sql.append(" ) AND device_id = '").append(deviceSchema.getDevice()).append("'");
    Map<String, String> tags = deviceSchema.getTags();
    if (tags != null) {
      for (Map.Entry<String, String> entry : tags.entrySet()) {
        sql.append(" AND ")
            .append(entry.getKey())
            .append(" = '")
            .append(entry.getValue())
            .append("'");
      }
    }
  }

  @Override
  public String getValueFilterClause(List<DeviceSchema> deviceSchemas, int valueThreshold) {
    DeviceSchema deviceSchema = deviceSchemas.get(0);
    StringBuilder builder = new StringBuilder();
    for (Sensor sensor : deviceSchema.getSensors()) {
      builder.append(" AND ").append(sensor.getName()).append(" > ");
      if (sensor.getSensorType() == SensorType.DATE) {
        builder
            .append("CAST(")
            .append("'")
            .append(LocalDate.ofEpochDay(Math.abs(valueThreshold)))
            .append("'")
            .append(" AS DATE)");
      } else {
        builder.append(valueThreshold);
      }
    }
    return builder.toString();
  }

  @Override
  public long getTimestamp(RowRecord rowRecord) {
    return rowRecord.getFields().get(0).getLongV();
  }

  @Override
  public int getQueryOffset() {
    return queryBaseOffset;
  }

  // TODO 用 count

  @Override
  public String getTotalLineNumberSql(DeviceSchema deviceSchema) {
    return "select * from "
        + dbConfig.getDB_NAME()
        + "_"
        + deviceSchema.getGroup()
        + "."
        + deviceSchema.getTable()
        + " where device_id = '"
        + deviceSchema.getDevice()
        + "'";
  }

  @Override
  public String getMaxTimeStampSql(DeviceSchema deviceSchema) {
    return "select * from "
        + dbConfig.getDB_NAME()
        + "_"
        + deviceSchema.getGroup()
        + "."
        + deviceSchema.getTable()
        + " order by time desc limit 1";
  }

  @Override
  public String getMinTimeStampSql(DeviceSchema deviceSchema) {
    return "select * from "
        + dbConfig.getDB_NAME()
        + "_"
        + deviceSchema.getGroup()
        + "."
        + deviceSchema.getTable()
        + " order by time limit 1";
  }

  // endregion

  // region insert

  @Override
  public Tablet createTablet(
      String insertTargetName,
      List<IMeasurementSchema> schemas,
      List<Tablet.ColumnType> columnTypes,
      int maxRowNumber) {
    return new Tablet(insertTargetName, schemas, columnTypes, maxRowNumber);
  }

  @Override
  public String getInsertTargetName(DeviceSchema schema) {
    return schema.getTable();
  }

  @Override
  public void addIDColumnIfNecessary(
      List<Tablet.ColumnType> columnTypes, List<Sensor> sensors, IBatch batch) {
    // All sensors are of type measurement
    for (int i = 0; i < sensors.size(); i++) {
      columnTypes.add(Tablet.ColumnType.MEASUREMENT);
    }
    // tag and device as ID column
    // Add Identity Column Information to Schema
    sensors.add(new Sensor("device_id", SensorType.STRING));
    columnTypes.add(Tablet.ColumnType.ID);
    for (String key : batch.getDeviceSchema().getTags().keySet()) {
      // Currently, the identity column can only be String
      sensors.add(new Sensor(key, SensorType.STRING));
      columnTypes.add(Tablet.ColumnType.ID);
    }
    // Add the value of the identity column to the value of each record
    for (int loop = 0; loop < config.getDEVICE_NUM_PER_WRITE(); loop++) {
      for (int i = 0; i < batch.getRecords().size(); i++) {
        List<Object> dataValue = batch.getRecords().get(i).getRecordDataValue();
        dataValue.add(batch.getDeviceSchema().getDevice());
        for (String key : batch.getDeviceSchema().getTags().keySet()) {
          dataValue.add(batch.getDeviceSchema().getTags().get(key));
        }
      }
      if (batch.hasNext()) {
        batch.next();
      }
    }
  }

  @Override
  public void deleteIDColumnIfNecessary(
      List<Tablet.ColumnType> columnTypes, List<Sensor> sensors, IBatch batch) {
    // do nothing
  }

  @Override
  public void sessionInsertImpl(Session session, Tablet tablet, DeviceSchema deviceSchema)
      throws IoTDBConnectionException, StatementExecutionException {
    if (session.getDatabase() == null) {
      StringBuilder sql = new StringBuilder();
      sql.append("use ").append(dbConfig.getDB_NAME()).append("_").append(deviceSchema.getGroup());
      session.executeNonQueryStatement(sql.toString());
    }
    session.insertRelationalTablet(tablet);
  }

  @Override
  public void sessionCleanupImpl(Session session)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement("show databases");
    while (dataSet.hasNext()) {
      String databaseName = dataSet.next().getFields().get(0).toString();
      if (databaseName.contains(".")) {
        continue;
      }
      session.executeNonQueryStatement("drop database " + databaseName);
    }
  }

  @Override
  public void addWhereValueClauseIfNecessary(List<DeviceSchema> devices, StringBuilder builder) {
    builder.append(" WHERE").append(getDeviceIDColumn(devices));
  }

  // endregion

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  @Override
  protected String getTimeArg(String aggFunction) {
    switch (aggFunction) {
      case Constants.FIRST_BY:
      case Constants.LAST_BY:
      case Constants.MAX_BY:
      case Constants.MIN_BY:
        return "time, ";
      default:
        return "";
    }
  }
}
