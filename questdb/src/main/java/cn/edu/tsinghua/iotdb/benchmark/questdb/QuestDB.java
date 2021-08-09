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

package cn.edu.tsinghua.iotdb.benchmark.questdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBUtil;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;

public class QuestDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(QuestDB.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final String URL_QUEST = "jdbc:postgresql://%s:%s/qdb";

  private static final String SSLMODE = "disable";

  private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS ";
  private static final String INSERT_SQL = "INSERT INTO ";
  private static final String DROP_TABLE = "DROP TABLE ";
  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private Connection connection = null;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  @Override
  public void init() throws TsdbException {
    try {
      Class.forName("org.postgresql.Driver");
      Properties properties = new Properties();
      properties.setProperty("user", config.getUSERNAME());
      properties.setProperty("password", config.getPASSWORD());
      properties.setProperty("sslmode", SSLMODE);
      properties.setProperty("gssEncMode", "disable");
      properties.setProperty("receiveBufferSize"," 1000");
      connection =
          DriverManager.getConnection(
              String.format(URL_QUEST, config.getHOST().get(0), config.getPORT().get(0)),
              properties);
    } catch (SQLException | ClassNotFoundException e) {
      e.printStackTrace();
      LOGGER.error("Failed to init database");
      throw new TsdbException("Failed to init database, maybe there is too much connections", e);
    }
  }

  /**
   * Cleanup any state for this DB, including the old data deletion. Called once before each test if
   * IS_DELETE_DATA=true.
   */
  @Override
  public void cleanup() throws TsdbException {
    try {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery("SHOW TABLES");
      while (resultSet.next()) {
        String table = resultSet.getString(1);
        if (table.startsWith(config.getDB_NAME())) {
          statement.addBatch(DROP_TABLE + table + ";");
        }
      }
      statement.executeBatch();
      statement.close();
      LOGGER.info("Clean up!");
    } catch (SQLException e) {
      LOGGER.error("Failed to cleanup!");
      throw new TsdbException("Failed to cleanup!", e);
    }
  }

  /** Close the DB instance connections. Called once per DB instance. */
  @Override
  public void close() throws TsdbException {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        LOGGER.warn("Failed to close connection");
        throw new TsdbException("Failed to close", e);
      }
    }
  }

  /**
   * Called once before each test if CREATE_SCHEMA=true.
   *
   * @param schemaList schema of devices to register
   */
  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    if (!config.getOPERATION_PROPORTION().split(":")[0].equals("0")) {
      try (Statement statement = connection.createStatement()) {
        for (DeviceSchema deviceSchema : schemaList) {
          StringBuffer create = new StringBuffer(CREATE_TABLE);
          // 添加表名
          create.append(config.getDB_NAME());
          create.append("_");
          create.append(deviceSchema.getGroup());
          create.append("_");
          create.append(deviceSchema.getDevice());
          // 添加时间戳
          create.append("( ts TIMESTAMP, ");
          // 添加传感器
          List<String> sensors = deviceSchema.getSensors();
          for (int index = 0; index < sensors.size(); index++) {
            String dataType = typeMap(DBUtil.getDataType(index));
            create.append(sensors.get(index));
            create.append(" ");
            create.append(dataType);
            if (index != sensors.size() - 1) {
              create.append(", ");
            }
          }
          // 声明主要的部分
          create.append(") timestamp(ts) ");
          create.append("PARTITION BY DAY WITH maxUncommittedRows=250000, commitLag=240s");
          statement.addBatch(create.toString());
        }
        statement.executeBatch();
      } catch (SQLException e) {
        // ignore if already has the time series
        LOGGER.error("Register TaosDB schema failed because ", e);
        throw new TsdbException(e);
      }
    }
  }

  /**
   * Insert one batch into the database, the DB implementation needs to resolve the data in batch
   * which contains device schema and Map[Long, List[String]] records. The key of records is a
   * timestamp and the value is a list of sensor value data.
   *
   * @param batch universal insertion data structure
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status insertOneBatch(Batch batch) throws DBConnectException {
    return insertBatch(batch);
  }

  /**
   * Insert single-sensor one batch into the database, the DB implementation needs to resolve the
   * data in batch which contains device schema and Map[Long, List[String]] records. The key of
   * records is a timestamp and the value is one sensor value data.
   *
   * @param batch universal insertion data structure
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status insertOneSensorBatch(Batch batch) throws DBConnectException {
    return insertBatch(batch);
  }

  private Status insertBatch(Batch batch) {
    try (Statement statement = connection.createStatement()) {
      DeviceSchema deviceSchema = batch.getDeviceSchema();
      StringBuffer tableName = new StringBuffer(config.getDB_NAME());
      tableName.append("_");
      tableName.append(deviceSchema.getGroup());
      tableName.append("_");
      tableName.append(deviceSchema.getDevice());
      for (Record record : batch.getRecords()) {
        StringBuffer insertSQL = new StringBuffer(INSERT_SQL);
        insertSQL.append(tableName);
        insertSQL.append(" values ('");
        insertSQL.append(sdf.format(record.getTimestamp()));
        insertSQL.append("'");
        for (int i = 0; i < record.getRecordDataValue().size(); i++) {
          Object value = record.getRecordDataValue().get(i);
          switch (typeMap(DBUtil.getDataType(i))) {
            case "BOOLEAN":
              insertSQL.append(",").append((boolean) value);
              break;
            case "INT":
              insertSQL.append(",").append((int) value);
              break;
            case "LONG":
              insertSQL.append(",").append((long) value);
              break;
            case "DOUBLE":
              insertSQL.append(",").append(Double.parseDouble(String.valueOf(value)));
              break;
            case "STRING":
            default:
              insertSQL.append(",").append("'").append((String) value).append("'");
              break;
          }
        }
        insertSQL.append(")");
        statement.addBatch(insertSQL.toString());
      }
      statement.executeBatch();
      statement.close();
      return new Status(true);
    } catch (SQLException e) {
      e.printStackTrace();
      System.out.println("Error!");
      return new Status(false, 0, e, e.toString());
    }
  }

  /**
   * Query data of one or multiple sensors at a precise timestamp. e.g. select v1... from data where
   * time = ? and device in ?
   *
   * @param preciseQuery universal precise query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    DeviceSchema targetDevice = preciseQuery.getDeviceSchema().get(0);
    List<String> sensors = targetDevice.getSensors();
    String table =
        config.getDB_NAME() + "_" + targetDevice.getGroup() + "_" + targetDevice.getDevice();
    String sqlHead = "SELECT " + sensors.get(0);
    for (int i = 1; i < sensors.size(); i++) {
      sqlHead += ", " + sensors.get(i);
    }
    String sql =
        sqlHead
            + " FROM "
            + table
            + " WHERE ts = '"
            + sdf.format(preciseQuery.getTimestamp())
            + "'";
    return executeQueryAndGetStatus(sql);
  }

  /**
   * Query data of one or multiple sensors in a time range. e.g. select v1... from data where time
   * >= ? and time <= ? and device in ?
   *
   * @param rangeQuery universal range query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    // select * from test_${group}_${device} where ts >= ? and ts <= ?;
    DeviceSchema targetDevice = rangeQuery.getDeviceSchema().get(0);
    List<String> sensors = targetDevice.getSensors();
    String table =
        config.getDB_NAME() + "_" + targetDevice.getGroup() + "_" + targetDevice.getDevice();
    String sqlHead = "SELECT " + sensors.get(0);
    for (int i = 1; i < sensors.size(); i++) {
      sqlHead += ", " + sensors.get(i);
    }
    String sql = sqlHead + " FROM " + table;
    sql = addWhereTimeClause(sql, rangeQuery);
    return executeQueryAndGetStatus(sql);
  }

  private static String addWhereTimeClause(String sql, RangeQuery rangeQuery) {
    String startTime = "" + sdf.format(rangeQuery.getStartTimestamp());
    String endTime = "" + sdf.format(rangeQuery.getEndTimestamp());
    return sql + " WHERE ts >= '" + startTime + "' AND ts <= '" + endTime + "'";
  }

  /**
   * Query data of one or multiple sensors in a time range with a value filter. e.g. select v1...
   * from data where time >= ? and time <= ? and v1 > ? and device in ?
   *
   * @param valueRangeQuery contains universal range query with value filter parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    // select * from test_${group}_${device} where ts >= ? and ts <= ? and s_${sensor} > ?;
    DeviceSchema targetDevice = valueRangeQuery.getDeviceSchema().get(0);
    List<String> sensors = targetDevice.getSensors();
    String table =
        config.getDB_NAME() + "_" + targetDevice.getGroup() + "_" + targetDevice.getDevice();
    String sqlHead = "SELECT " + sensors.get(0);
    for (int i = 1; i < sensors.size(); i++) {
      sqlHead += ", " + sensors.get(i);
    }
    sqlHead += " FROM " + table;
    String sqlWithTimeFilter = addWhereTimeClause(sqlHead, valueRangeQuery);
    String sql =
        addWhereValueClause(
            valueRangeQuery.getDeviceSchema(),
            sqlWithTimeFilter,
            valueRangeQuery.getValueThreshold());
    return executeQueryAndGetStatus(sql);
  }

  private static String addWhereValueClause(
      List<DeviceSchema> devices, String sql, double valueThreshold) {
    StringBuilder builder = new StringBuilder(sql);
    for (String sensor : devices.get(0).getSensors()) {
      builder.append(" AND ").append(sensor).append(" > ").append(valueThreshold);
    }
    return builder.toString();
  }

  /**
   * Query aggregated data of one or multiple sensors in a time range using aggregation function.
   * e.g. select func(v1)... from data where device in ? and time >= ? and time <= ?
   *
   * @param aggRangeQuery contains universal aggregation query with time filter parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    DeviceSchema targetDevice = aggRangeQuery.getDeviceSchema().get(0);
    List<String> sensors = targetDevice.getSensors();
    String table =
        config.getDB_NAME() + "_" + targetDevice.getGroup() + "_" + targetDevice.getDevice();
    String aggQuerySqlHead =
        getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(), aggRangeQuery.getAggFun());
    aggQuerySqlHead += " FROM " + table;
    String sql = addWhereTimeClause(aggQuerySqlHead, aggRangeQuery);
    return executeQueryAndGetStatus(sql);
  }

  private static String getAggQuerySqlHead(List<DeviceSchema> devices, String method) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    builder.append(method).append("()");
    return builder.toString();
  }

  /**
   * Query aggregated data of one or multiple sensors in the whole time range. e.g. select
   * func(v1)... from data where device in ? and value > ?
   *
   * @param aggValueQuery contains universal aggregation query with value filter parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    DeviceSchema targetDevice = aggValueQuery.getDeviceSchema().get(0);
    List<String> sensors = targetDevice.getSensors();
    String table =
        config.getDB_NAME() + "_" + targetDevice.getGroup() + "_" + targetDevice.getDevice();
    String aggQuerySqlHead =
        getAggQuerySqlHead(aggValueQuery.getDeviceSchema(), aggValueQuery.getAggFun());
    aggQuerySqlHead += " FROM " + table;
    String sql =
        addWhereValueWithoutTimeClause(
            aggValueQuery.getDeviceSchema(), aggQuerySqlHead, aggValueQuery.getValueThreshold());
    return executeQueryAndGetStatus(sql);
  }

  private static String addWhereValueWithoutTimeClause(
      List<DeviceSchema> devices, String sqlHeader, double valueThreshold) {
    StringBuilder builder = new StringBuilder(sqlHeader);
    builder.append(" WHERE ");
    for (String sensor : devices.get(0).getSensors()) {
      builder.append(sensor).append(" > ").append(valueThreshold).append(" AND ");
    }
    builder.delete(builder.lastIndexOf("AND"), builder.length());
    return builder.toString();
  }

  /**
   * Query aggregated data of one or multiple sensors with both time and value filters. e.g. select
   * func(v1)... from data where device in ? and time >= ? and time <= ? and value > ?
   *
   * @param aggRangeValueQuery contains universal aggregation query with time and value filters
   *     parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    DeviceSchema targetDevice = aggRangeValueQuery.getDeviceSchema().get(0);
    List<String> sensors = targetDevice.getSensors();
    String table =
        config.getDB_NAME() + "_" + targetDevice.getGroup() + "_" + targetDevice.getDevice();
    String aggQuerySqlHead =
        getAggQuerySqlHead(aggRangeValueQuery.getDeviceSchema(), aggRangeValueQuery.getAggFun());
    aggQuerySqlHead += " FROM " + table;
    String sqlWithTimeFilter = addWhereTimeClause(aggQuerySqlHead, aggRangeValueQuery);
    String sql =
        addWhereValueClause(
            aggRangeValueQuery.getDeviceSchema(),
            sqlWithTimeFilter,
            aggRangeValueQuery.getValueThreshold());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * Query aggregated group-by-time data of one or multiple sensors within a time range. e.g. SELECT
   * max(s_0), max(s_1) FROM group_0, group_1 WHERE ( device = ’d_3’ OR device = ’d_8’) AND time >=
   * 2010-01-01 12:00:00 AND time <= 2010-01-01 12:10:00 GROUP BY time(60000ms)
   *
   * @param groupByQuery contains universal group by query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    DeviceSchema targetDevice = groupByQuery.getDeviceSchema().get(0);
    List<String> sensors = targetDevice.getSensors();
    String table =
        config.getDB_NAME() + "_" + targetDevice.getGroup() + "_" + targetDevice.getDevice();
    String aggQuerySqlHead =
        getAggQuerySqlHead(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun());
    aggQuerySqlHead += " FROM " + table;
    String sqlWithTimeFilter = addWhereTimeClause(aggQuerySqlHead, groupByQuery);
    String sqlWithGroupBy = addGroupByClause(sqlWithTimeFilter, groupByQuery.getGranularity());
    return executeQueryAndGetStatus(sqlWithGroupBy);
  }

  private static String addGroupByClause(String sqlHeader, long timeGranularity) {
    return sqlHeader + " GROUP BY ts(" + timeGranularity + ")";
  }
  /**
   * Query the latest(max-timestamp) data of one or multiple sensors. e.g. select time, v1... where
   * device = ? and time = max(time)
   *
   * @param latestPointQuery contains universal latest point query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    DeviceSchema targetDevice = latestPointQuery.getDeviceSchema().get(0);
    List<String> sensors = targetDevice.getSensors();
    String table =
        config.getDB_NAME() + "_" + targetDevice.getGroup() + "_" + targetDevice.getDevice();
    String sql = "SELECT " + sensors.get(0) + " FROM " + table + " LATEST BY " + sensors.get(0);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * similar to rangeQuery, but order by time desc.
   *
   * @param rangeQuery
   */
  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    DeviceSchema targetDevice = rangeQuery.getDeviceSchema().get(0);
    List<String> sensors = targetDevice.getSensors();
    String table =
        config.getDB_NAME() + "_" + targetDevice.getGroup() + "_" + targetDevice.getDevice();
    String sqlHead = "SELECT " + sensors.get(0);
    for (int i = 1; i < sensors.size(); i++) {
      sqlHead += ", " + sensors.get(i);
    }
    String sql = sqlHead + " FROM " + table;
    sql = addWhereTimeClause(sql, rangeQuery) + " ORDER BY ts DESC";
    return executeQueryAndGetStatus(sql);
  }

  /**
   * similar to valueRangeQuery, but order by time desc.
   *
   * @param valueRangeQuery
   */
  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    DeviceSchema targetDevice = valueRangeQuery.getDeviceSchema().get(0);
    List<String> sensors = targetDevice.getSensors();
    String table =
        config.getDB_NAME() + "_" + targetDevice.getGroup() + "_" + targetDevice.getDevice();
    String sqlHead = "SELECT " + sensors.get(0);
    for (int i = 1; i < sensors.size(); i++) {
      sqlHead += ", " + sensors.get(i);
    }
    sqlHead += " FROM " + table;
    String sqlWithTimeFilter = addWhereTimeClause(sqlHead, valueRangeQuery);
    String sql =
        addWhereValueClause(
                valueRangeQuery.getDeviceSchema(),
                sqlWithTimeFilter,
                valueRangeQuery.getValueThreshold())
            + " ORDER BY ts DESC";
    ;
    return executeQueryAndGetStatus(sql);
  }

  private Status executeQueryAndGetStatus(String sql) {
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), sql);
    }
    LOGGER.debug("execute sql {}", sql);
    int line = 0;
    int queryResultPointNum = 0;
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      while (resultSet.next()) {
        line++;
      }
      queryResultPointNum = line * config.getQUERY_SENSOR_NUM() * config.getDEVICE_NUMBER();
      return new Status(true, queryResultPointNum);
    } catch (SQLException e) {
      e.printStackTrace();
      return new Status(false, 0, e, e.toString());
    }
  }

  /**
   * map the given type string name to the name in the target DB
   *
   * @param iotdbType : "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"
   * @return
   */
  @Override
  public String typeMap(String iotdbType) {
    switch (iotdbType) {
      case "BOOLEAN":
        return "BOOLEAN";
      case "INT32":
        return "INT";
      case "INT64":
        return "LONG";
      case "FLOAT":
      case "DOUBLE":
        return "DOUBLE";
      case "TEXT":
        return "STRING";
      default:
        LOGGER.error("Unsupported data type {}, use default data type: BINARY.", iotdbType);
        return "STRING";
    }
  }
}
