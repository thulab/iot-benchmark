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

package cn.edu.tsinghua.iot.benchmark.iotdb200;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SQLDialect;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy.DMLStrategy;
import cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy.JDBCStrategy;
import cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy.SessionStrategy;
import cn.edu.tsinghua.iot.benchmark.iotdb200.ModelStrategy.IoTDBModelStrategy;
import cn.edu.tsinghua.iot.benchmark.iotdb200.ModelStrategy.TableStrategy;
import cn.edu.tsinghua.iot.benchmark.iotdb200.ModelStrategy.TreeStrategy;
import cn.edu.tsinghua.iot.benchmark.iotdb200.utils.IoTDBUtils;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.BlobUtils;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.DeviceQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.VerificationQuery;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** this class will create more than one connection. */
public class IoTDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);

  public static String DELETE_SERIES_SQL;
  public static String ROOT_SERIES_NAME;
  private final DBConfig dbConfig;
  private final Random random = new Random(config.getDATA_SEED());
  private final DMLStrategy dmlStrategy;
  private final IoTDBModelStrategy modelStrategy;

  public static final String ALREADY_KEYWORD = "already";
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final String ORDER_BY_TIME_DESC = " order by time desc ";

  public IoTDB(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
    ROOT_SERIES_NAME = "root." + dbConfig.getDB_NAME();
    DELETE_SERIES_SQL = "delete storage group root." + dbConfig.getDB_NAME() + ".*";
    // init IoTDBModelStrategy and DMLStrategy
    modelStrategy =
        config.getIoTDB_DIALECT_MODE() == SQLDialect.TABLE
            ? new TableStrategy(dbConfig)
            : new TreeStrategy(dbConfig);
    switch (dbConfig.getDB_SWITCH()) {
      case DB_IOT_200_REST:
      case DB_IOT_200_SESSION_BY_TABLET:
      case DB_IOT_200_SESSION_BY_RECORD:
      case DB_IOT_200_SESSION_BY_RECORDS:
        dmlStrategy = new SessionStrategy(this, dbConfig);
        break;
      case DB_IOT_200_JDBC:
        dmlStrategy = new JDBCStrategy(dbConfig);
        break;
      default:
        throw new IllegalArgumentException("Unsupported DB SWITCH: " + dbConfig.getDB_SWITCH());
    }
  }

  @Override
  public void init() throws TsdbException {
    dmlStrategy.init();
  }

  @Override
  public void cleanup() throws TsdbException {
    dmlStrategy.cleanup();
  }

  @Override
  public void close() throws TsdbException {
    dmlStrategy.close();
  }

  /**
   * create timeseries one by one is too slow in current cluster server. therefore, we use session
   * to create time series in batch.
   *
   * @param schemaList schema of devices to register
   */
  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    long start = System.nanoTime();
    if (config.hasWrite()) {
      Map<Session, List<TimeseriesSchema>> sessionListMap = new HashMap<>();
      try {
        // open meta session
        Session metaSession =
            new Session.Builder()
                .host(dbConfig.getHOST().get(0))
                .port(Integer.parseInt(dbConfig.getPORT().get(0)))
                .username(dbConfig.getUSERNAME())
                .password(dbConfig.getPASSWORD())
                .version(Version.V_1_0)
                .sqlDialect(config.getIoTDB_DIALECT_MODE().name())
                .build();
        metaSession.open(config.isENABLE_THRIFT_COMPRESSION());
        sessionListMap.put(metaSession, createTimeseries(schemaList));
        modelStrategy.registerSchema(sessionListMap, schemaList);
      } catch (Exception e) {
        throw new TsdbException(e);
      } finally {
        if (!sessionListMap.isEmpty()) {
          Set<Session> sessions = sessionListMap.keySet();
          for (Session session : sessions) {
            try {
              session.close();
            } catch (IoTDBConnectionException e) {
              LOGGER.error("Schema-register session cannot be closed: {}", e.getMessage());
            }
          }
        }
      }
    }
    long end = System.nanoTime();
    return TimeUtils.convertToSeconds(end - start, "ns");
  }

  private List<TimeseriesSchema> createTimeseries(List<DeviceSchema> schemaList) {
    List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
    for (DeviceSchema deviceSchema : schemaList) {
      TimeseriesSchema timeseriesSchema = createTimeseries(deviceSchema);
      timeseriesSchemas.add(timeseriesSchema);
    }
    return timeseriesSchemas;
  }

  private TimeseriesSchema createTimeseries(DeviceSchema deviceSchema) {
    List<String> paths = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    List<TSEncoding> tsEncodings = new ArrayList<>();
    List<CompressionType> compressionTypes = new ArrayList<>();
    for (Sensor sensor : deviceSchema.getSensors()) {
      if (config.isVECTOR()) {
        paths.add(sensor.getName());
      } else {
        paths.add(getSensorPath(deviceSchema, sensor.getName()));
      }
      SensorType datatype = sensor.getSensorType();
      tsDataTypes.add(Enum.valueOf(TSDataType.class, datatype.name));
      tsEncodings.add(
          Enum.valueOf(TSEncoding.class, Objects.requireNonNull(IoTDB.getEncodingType(datatype))));
      compressionTypes.add(Enum.valueOf(CompressionType.class, config.getCOMPRESSOR()));
    }
    TimeseriesSchema timeseriesSchema =
        new TimeseriesSchema(deviceSchema, paths, tsDataTypes, tsEncodings, compressionTypes);
    if (config.isVECTOR()) {
      timeseriesSchema.setDeviceId(IoTDBUtils.getDevicePath(deviceSchema, ROOT_SERIES_NAME));
    }
    return timeseriesSchema;
  }

  @Override
  public Status insertOneBatch(IBatch batch) throws DBConnectException {
    String deviceId = IoTDBUtils.getDevicePath(batch.getDeviceSchema(), ROOT_SERIES_NAME);
    return dmlStrategy.insertOneBatch(batch, deviceId);
  }

  /**
   * Q1: PreciseQuery SQL: select {sensors} from {devices} where time = {time}
   *
   * @param preciseQuery universal precise query condition parameters
   */
  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    String strTime = preciseQuery.getTimestamp() + "";
    String sql = getSimpleQuerySqlHead(preciseQuery.getDeviceSchema()) + " WHERE time = " + strTime;
    sql = modelStrategy.addDeviceIDColumnIfNecessary(preciseQuery.getDeviceSchema(), sql);
    return executeQueryAndGetStatus(sql, Operation.PRECISE_QUERY);
  }

  /**
   * Q2: RangeQuery SQL: select {sensors} from {devices} where time >= {startTime} and time <=
   * {endTime}
   *
   * @param rangeQuery universal range query condition parameters
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    String sql =
        getRangeQuerySql(
            rangeQuery.getDeviceSchema(),
            rangeQuery.getStartTimestamp(),
            rangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(sql, Operation.RANGE_QUERY);
  }

  /**
   * Q3: ValueRangeQuery SQL: select {sensors} from {devices} where time >= {startTime} and time <=
   * {endTime} and {sensors} > {value}
   *
   * @param valueRangeQuery contains universal range query with value filter parameters
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    String sql = getValueRangeQuerySql(valueRangeQuery);
    return executeQueryAndGetStatus(sql, Operation.VALUE_RANGE_QUERY);
  }

  /**
   * Q4: AggRangeQuery SQL: select {AggFun}({sensors}) from {devices} where time >= {startTime} and
   * time <= {endTime}
   *
   * @param aggRangeQuery contains universal aggregation query with time filter parameters
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    String aggQuerySqlHead =
        getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(), aggRangeQuery.getAggFun());
    String sql =
        addWhereTimeClause(
            aggQuerySqlHead, aggRangeQuery.getStartTimestamp(), aggRangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(sql, Operation.AGG_RANGE_QUERY);
  }

  /**
   * Q5: AggValueQuery SQL: select {AggFun}({sensors}) from {devices} where {sensors} > {value}
   *
   * @param aggValueQuery contains universal aggregation query with value filter parameters
   */
  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    String aggQuerySqlHead =
        getAggQuerySqlHead(aggValueQuery.getDeviceSchema(), aggValueQuery.getAggFun());
    String sql =
        aggQuerySqlHead
            + " WHERE "
            + getValueFilterClause(
                    aggValueQuery.getDeviceSchema(), (int) aggValueQuery.getValueThreshold())
                .substring(4);
    return executeQueryAndGetStatus(sql, Operation.AGG_VALUE_QUERY);
  }

  /**
   * Q6: AggRangeValueQuery SQL: select {AggFun}({sensors}) from {devices} where time >= {startTime}
   * and time <= {endTime} and {sensors} > {value}
   *
   * @param aggRangeValueQuery contains universal aggregation query with time and value filters
   *     parameters
   */
  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    String aggQuerySqlHead =
        getAggQuerySqlHead(aggRangeValueQuery.getDeviceSchema(), aggRangeValueQuery.getAggFun());
    String sql =
        addWhereTimeClause(
            aggQuerySqlHead,
            aggRangeValueQuery.getStartTimestamp(),
            aggRangeValueQuery.getEndTimestamp());
    sql +=
        getValueFilterClause(
            aggRangeValueQuery.getDeviceSchema(), (int) aggRangeValueQuery.getValueThreshold());
    return executeQueryAndGetStatus(sql, Operation.AGG_RANGE_VALUE_QUERY);
  }

  /**
   * Q7: GroupByQuery SQL: select {AggFun}({sensors}) from {devices} group by ([{start}, {end}],
   * {Granularity}ms)
   *
   * @param groupByQuery contains universal group by query condition parameters
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    String aggQuerySqlHead =
        getAggQuerySqlHead(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun());
    String sql =
        addGroupByClause(
            aggQuerySqlHead,
            groupByQuery.getStartTimestamp(),
            groupByQuery.getEndTimestamp(),
            groupByQuery.getGranularity());
    return executeQueryAndGetStatus(sql, Operation.GROUP_BY_QUERY);
  }

  /**
   * Q8: LatestPointQuery SQL: select last {sensors} from {devices}
   *
   * @param latestPointQuery contains universal latest point query condition parameters
   */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    String aggQuerySqlHead = getLatestPointQuerySql(latestPointQuery.getDeviceSchema());
    return executeQueryAndGetStatus(aggQuerySqlHead, Operation.LATEST_POINT_QUERY);
  }

  /**
   * Q9: RangeQuery SQL: select {sensors} from {devices} where time >= {startTime} and time <=
   * {endTime} order by time desc
   *
   * @param rangeQuery universal range query condition parameters
   */
  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    String sql =
        getRangeQuerySql(
                rangeQuery.getDeviceSchema(),
                rangeQuery.getStartTimestamp(),
                rangeQuery.getEndTimestamp())
            + " order by time desc";
    return executeQueryAndGetStatus(sql, Operation.RANGE_QUERY_ORDER_BY_TIME_DESC);
  }

  /**
   * Q10: ValueRangeQuery SQL: select {sensors} from {devices} where time >= {startTime} and time <=
   * {endTime} and {sensors} > {value} order by time desc
   *
   * @param valueRangeQuery contains universal range query with value filter parameters
   */
  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    String sql = getValueRangeQuerySql(valueRangeQuery) + " order by time desc";
    return executeQueryAndGetStatus(sql, Operation.VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC);
  }

  @Override
  public Status groupByQueryOrderByDesc(GroupByQuery groupByQuery) {
    String aggQuerySqlHead =
        getAggQuerySqlHead(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun());
    String sql =
        addGroupByClause(
            aggQuerySqlHead,
            groupByQuery.getStartTimestamp(),
            groupByQuery.getEndTimestamp(),
            groupByQuery.getGranularity());
    sql += ORDER_BY_TIME_DESC;
    return executeQueryAndGetStatus(sql, Operation.GROUP_BY_QUERY_ORDER_BY_TIME_DESC);
  }

  /**
   * Generate simple query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. Select sensors from devices
   */
  protected String getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    builder.append(modelStrategy.selectTimeColumnIfNecessary());
    List<Sensor> querySensors = devices.get(0).getSensors();
    builder.append(querySensors.get(0).getName());
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i).getName());
    }
    return addFromClause(devices, builder);
  }

  private String getAggQuerySqlHead(List<DeviceSchema> devices, String aggFun) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<Sensor> querySensors = devices.get(0).getSensors();
    builder.append(aggFun).append("(").append(querySensors.get(0).getName()).append(")");
    for (int i = 1; i < querySensors.size(); i++) {
      builder
          .append(", ")
          .append(aggFun)
          .append("(")
          .append(querySensors.get(i).getName())
          .append(")");
    }
    return addFromClause(devices, builder);
  }

  /**
   * Add from Clause
   *
   * @return From clause, e.g. FROM devices
   */
  private String addFromClause(List<DeviceSchema> devices, StringBuilder builder) {
    return modelStrategy.addFromClause(devices, builder);
  }

  private String getValueRangeQuerySql(ValueRangeQuery valueRangeQuery) {
    String rangeQuerySql =
        getRangeQuerySql(
            valueRangeQuery.getDeviceSchema(),
            valueRangeQuery.getStartTimestamp(),
            valueRangeQuery.getEndTimestamp());
    String valueFilterClause =
        getValueFilterClause(
            valueRangeQuery.getDeviceSchema(), (int) valueRangeQuery.getValueThreshold());
    return rangeQuerySql + valueFilterClause;
  }

  private String getValueFilterClause(List<DeviceSchema> deviceSchemas, int valueThreshold) {
    StringBuilder builder = new StringBuilder();
    modelStrategy.getValueFilterClause(deviceSchemas, valueThreshold, builder);
    return builder.toString();
  }

  private String getLatestPointQuerySql(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT last ");
    List<Sensor> querySensors = devices.get(0).getSensors();
    builder.append(querySensors.get(0).getName());
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i).getName());
    }
    return addFromClause(devices, builder);
  }

  private String getRangeQuerySql(List<DeviceSchema> deviceSchemas, long start, long end) {
    return modelStrategy.addDeviceIDColumnIfNecessary(
        deviceSchemas, addWhereTimeClause(getSimpleQuerySqlHead(deviceSchemas), start, end));
  }

  private String addWhereTimeClause(String prefix, long start, long end) {
    String startTime = start + "";
    String endTime = end + "";
    return prefix + " WHERE time >= " + startTime + " AND time <= " + endTime;
  }

  private String addGroupByClause(String prefix, long start, long end, long granularity) {
    return prefix + " group by ([" + start + "," + end + ")," + granularity + "ms) ";
  }

  protected Status executeQueryAndGetStatus(String sql, Operation operation) {
    String executeSQL;
    if (config.isIOTDB_USE_DEBUG() && random.nextDouble() < config.getIOTDB_USE_DEBUG_RATIO()) {
      executeSQL = "debug " + sql;
    } else {
      executeSQL = sql;
    }
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), executeSQL);
    }

    long queryResultPointNum = 0;
    AtomicBoolean isOk = new AtomicBoolean(true);
    List<List<Object>> records = new ArrayList<>();
    try {
      queryResultPointNum =
          dmlStrategy.executeQueryAndGetStatusImpl(executeSQL, operation, isOk, records);
    } catch (Throwable t) {
      return new Status(false, queryResultPointNum, new Exception(t), executeSQL);
    }
    if (isOk.get()) {
      if (config.isIS_COMPARISON()) {
        return new Status(true, queryResultPointNum, executeSQL, records);
      } else {
        return new Status(true, queryResultPointNum);
      }
    } else {
      return new Status(
          false, queryResultPointNum, new Exception("Failed to execute."), executeSQL);
    }
  }

  /** Using in verification */
  @Override
  public Status verificationQuery(VerificationQuery verificationQuery) {
    DeviceSchema deviceSchema = verificationQuery.getDeviceSchema();
    List<DeviceSchema> deviceSchemas = new ArrayList<>();
    deviceSchemas.add(deviceSchema);

    List<Record> records = new ArrayList<>();
    List<TSDataType> tsDataTypes =
        IoTDBUtils.constructDataTypes(deviceSchema.getSensors(), deviceSchema.getSensors().size());
    for (Record record : verificationQuery.getRecords()) {
      records.add(
          new Record(record.getTimestamp(), convertTypeForBlobAndDate(record, tsDataTypes)));
    }
    if (records.isEmpty()) {
      return new Status(
          false,
          new TsdbException("There are no records in verficationQuery."),
          "There are no records in verficationQuery.");
    }

    StringBuffer sql = new StringBuffer();
    sql.append(getSimpleQuerySqlHead(deviceSchemas));
    Map<Long, List<Object>> recordMap = new HashMap<>();
    modelStrategy.addVerificationQueryWhereClause(sql, records, recordMap, deviceSchema);
    int point, line;
    try {
      List<Integer> resultList = dmlStrategy.verificationQueryImpl(sql.toString(), recordMap);
      point = resultList.get(0);
      line = resultList.get(1);
    } catch (Exception e) {
      LOGGER.error("Query Error: {}", sql, e);
      return new Status(false, new TsdbException("Failed to query"), "Failed to query.");
    }
    if (recordMap.size() != line) {
      LOGGER.error("Using SQL: {},Expected line:{} but was: {}", sql, recordMap.size(), line);
    }
    return new Status(true, point);
  }

  private List<Object> convertTypeForBlobAndDate(Record record, List<TSDataType> dataTypes) {
    List<Object> dataValue = record.getRecordDataValue();
    for (int recordValueIndex = 0;
        recordValueIndex < record.getRecordDataValue().size();
        recordValueIndex++) {
      switch (dataTypes.get(recordValueIndex)) {
        case BLOB:
          // "7I" to "0x3749"
          dataValue.set(
              recordValueIndex,
              "0x"
                  + BlobUtils.stringToHex(
                          (String) record.getRecordDataValue().get(recordValueIndex))
                      .toLowerCase());
          break;
        case DATE:
          // "2024-04-07" to "20240407"
          String value = record.getRecordDataValue().get(recordValueIndex).toString();
          value = value.substring(0, 4) + value.substring(5, 7) + value.substring(8);
          dataValue.set(recordValueIndex, value);
          break;
      }
    }
    return dataValue;
  }

  @Override
  public Status deviceQuery(DeviceQuery deviceQuery) throws SQLException, TsdbException {
    DeviceSchema deviceSchema = deviceQuery.getDeviceSchema();
    String sql =
        getDeviceQuerySql(
            deviceSchema, deviceQuery.getStartTimestamp(), deviceQuery.getEndTimestamp());
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("Query: {}", sql);
    }
    List<List<Object>> result;
    try {
      result = dmlStrategy.deviceQueryImpl(sql);
    } catch (Exception e) {
      LOGGER.error("Query Error: {}", sql, e);
      return new Status(false, new TsdbException("Failed to query"), "Failed to query.");
    }
    return new Status(true, 0, sql, result);
  }

  protected String getDeviceQuerySql(
      DeviceSchema deviceSchema, long startTimeStamp, long endTimeStamp) {
    StringBuffer sql = new StringBuffer();
    List<DeviceSchema> deviceSchemas = new ArrayList<>();
    deviceSchemas.add(deviceSchema);
    sql.append(getSimpleQuerySqlHead(deviceSchemas));
    sql.append(" where time >= ").append(startTimeStamp);
    sql.append(" and time <").append(endTimeStamp);
    sql.append(" order by time desc");
    return sql.toString();
  }

  @Override
  public DeviceSummary deviceSummary(DeviceQuery deviceQuery) throws SQLException, TsdbException {
    DeviceSchema schema = deviceQuery.getDeviceSchema();
    return dmlStrategy.deviceSummary(
        schema.getDevice(),
        modelStrategy.getTotalLineNumberSql(schema),
        modelStrategy.getMaxTimeStampSql(schema),
        modelStrategy.getMinTimeStampSql(schema));
  }

  @Override
  public String typeMap(SensorType iotdbSensorType) {
    return IDatabase.super.typeMap(iotdbSensorType);
  }

  public static String getEncodingType(SensorType dataSensorType) {
    switch (dataSensorType) {
      case BOOLEAN:
        return config.getENCODING_BOOLEAN();
      case INT32:
        return config.getENCODING_INT32();
      case INT64:
        return config.getENCODING_INT64();
      case FLOAT:
        return config.getENCODING_FLOAT();
      case DOUBLE:
        return config.getENCODING_DOUBLE();
      case TEXT:
        return config.getENCODING_TEXT();
      case STRING:
        return config.getENCODING_STRING();
      case BLOB:
        return config.getENCODING_BLOB();
      case TIMESTAMP:
        return config.getENCODING_TIMESTAMP();
      case DATE:
        return config.getENCODING_DATE();
      default:
        LOGGER.error("Unsupported data sensorType {}.", dataSensorType);
        return null;
    }
  }

  /** convert deviceSchema and sensor to the format: root.group_1.d_1.s_1 */
  public static String getSensorPath(DeviceSchema deviceSchema, String sensor) {
    return IoTDBUtils.getDevicePath(deviceSchema, ROOT_SERIES_NAME) + "." + sensor;
  }

  public String getInsertTargetName(DeviceSchema schema) {
    return modelStrategy.getInsertTargetName(schema);
  }

  public Tablet createTablet(
      String insertTargetName,
      List<IMeasurementSchema> schemas,
      List<Tablet.ColumnType> columnTypes,
      int maxRowNumber) {
    return modelStrategy.createTablet(insertTargetName, schemas, columnTypes, maxRowNumber);
  }

  public void sessionCleanupImpl(Session session)
      throws IoTDBConnectionException, StatementExecutionException {
    modelStrategy.sessionCleanupImpl(session);
  }

  public void sessionInsertImpl(Session session, Tablet tablet, DeviceSchema deviceSchema)
      throws IoTDBConnectionException, StatementExecutionException {
    modelStrategy.sessionInsertImpl(session, tablet, deviceSchema);
  }

  public void addIDColumnIfNecessary(
      List<Tablet.ColumnType> columnTypes, List<Sensor> sensors, IBatch batch) {
    modelStrategy.addIDColumnIfNecessary(columnTypes, sensors, batch);
  }

  public void deleteIDColumnIfNecessary(
      List<Tablet.ColumnType> columnTypes, List<Sensor> sensors, IBatch batch) {
    modelStrategy.deleteIDColumnIfNecessary(columnTypes, sensors, batch);
  }

  public long getTimestamp(RowRecord rowRecord) {
    return modelStrategy.getTimestamp(rowRecord);
  }

  public String getValue(RowRecord rowRecord, int i) {
    return rowRecord.getFields().get(i + modelStrategy.getQueryOffset()).toString();
  }
}