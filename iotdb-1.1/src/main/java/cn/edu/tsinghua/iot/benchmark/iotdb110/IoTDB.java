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

package cn.edu.tsinghua.iot.benchmark.iotdb110;

import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.NamedThreadFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** this class will create more than one connection. */
public class IoTDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  private static final String ALREADY_KEYWORD = "already";
  private static final AtomicBoolean templateInit = new AtomicBoolean(false);
  private static final int ACTIVATE_TEMPLATE_THRESHOLD = 1000;
  protected final String DELETE_SERIES_SQL;
  private final String ORDER_BY_TIME_DESC = " order by time desc ";
  protected SingleNodeJDBCConnection ioTDBConnection;

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected static final CyclicBarrier templateBarrier =
      new CyclicBarrier(config.getCLIENT_NUMBER());
  protected static final CyclicBarrier schemaBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());
  protected static final CyclicBarrier activateTemplateBarrier =
      new CyclicBarrier(config.getCLIENT_NUMBER());
  protected static Set<String> storageGroups = Collections.synchronizedSet(new HashSet<>());
  protected final String ROOT_SERIES_NAME;
  protected ExecutorService service;
  protected Future<?> task;
  protected DBConfig dbConfig;
  protected Random random = new Random(config.getDATA_SEED());

  public IoTDB(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
    ROOT_SERIES_NAME = "root." + dbConfig.getDB_NAME();
    DELETE_SERIES_SQL = "delete storage group root." + dbConfig.getDB_NAME() + ".*";
  }

  @Override
  public void init() throws TsdbException {
    if (ioTDBConnection == null) {
      try {
        ioTDBConnection = new SingleNodeJDBCConnection(dbConfig);
        ioTDBConnection.init();
        this.service =
            Executors.newSingleThreadExecutor(new NamedThreadFactory("DataClientExecuteJob"));
      } catch (Exception e) {
        throw new TsdbException(e);
      }
    }
  }

  @Override
  public void cleanup() {
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      statement.execute(DELETE_SERIES_SQL);
      LOGGER.info("Finish clean data!");
    } catch (Exception e) {
      LOGGER.warn("No Data to Clean!");
    }
  }

  @Override
  public void close() throws TsdbException {
    if (ioTDBConnection != null) {
      ioTDBConnection.close();
    }
    if (service != null) {
      service.shutdownNow();
    }
    if (task != null) {
      task.cancel(true);
    }
  }

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    // create timeseries one by one is too slow in current cluster server.
    // therefore, we use session to create time series in batch.
    long start = System.nanoTime();
    long end;
    if (config.hasWrite()) {
      Map<Session, List<TimeseriesSchema>> sessionListMap = new HashMap<>();
      try {
        // open meta session
        if (!config.isIS_ALL_NODES_VISIBLE()) {
          Session metaSession =
              new Session.Builder()
                  .host(dbConfig.getHOST().get(0))
                  .port(Integer.parseInt(dbConfig.getPORT().get(0)))
                  .username(dbConfig.getUSERNAME())
                  .password(dbConfig.getPASSWORD())
                  .version(Version.V_1_0)
                  .build();
          metaSession.open(config.isENABLE_THRIFT_COMPRESSION());
          sessionListMap.put(metaSession, createTimeseries(schemaList));
        } else {
          int sessionNumber = dbConfig.getHOST().size();
          List<Session> keys = new ArrayList<>();
          for (int i = 0; i < sessionNumber; i++) {
            Session metaSession =
                new Session.Builder()
                    .host(dbConfig.getHOST().get(i))
                    .port(Integer.parseInt(dbConfig.getPORT().get(i)))
                    .username(dbConfig.getUSERNAME())
                    .password(dbConfig.getPASSWORD())
                    .version(Version.V_1_0)
                    .build();
            metaSession.open(config.isENABLE_THRIFT_COMPRESSION());
            keys.add(metaSession);
            sessionListMap.put(metaSession, new ArrayList<>());
          }
          for (int i = 0; i < schemaList.size(); i++) {
            sessionListMap
                .get(keys.get(i % sessionNumber))
                .add(createTimeseries(schemaList.get(i)));
          }
        }

        if (config.isTEMPLATE()
            && schemaList.size() > 0
            && templateInit.compareAndSet(false, true)) {
          Template template = createTemplate(schemaList.get(0));
          start = System.nanoTime();
          int sessionIndex = random.nextInt(sessionListMap.size());
          Session templateSession = new ArrayList<>(sessionListMap.keySet()).get(sessionIndex);
          registerTemplate(templateSession, template);
        } else {
          start = System.nanoTime();
        }
        templateBarrier.await();
        for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
          registerStorageGroups(pair.getKey(), pair.getValue());
        }
        schemaBarrier.await();
        if (config.isTEMPLATE()) {
          for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
            activateTemplate(pair.getKey(), pair.getValue());
          }
          activateTemplateBarrier.await();
        }
        if (!config.isTEMPLATE()) {
          for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
            registerTimeseries(pair.getKey(), pair.getValue());
          }
        }
      } catch (Exception e) {
        throw new TsdbException(e);
      } finally {
        if (sessionListMap.size() != 0) {
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
    end = System.nanoTime();
    return TimeUtils.convertToSeconds(end - start, "ns");
  }

  /** create template */
  private Template createTemplate(DeviceSchema deviceSchema) {
    Template template = null;
    if (config.isTEMPLATE()) {
      if (config.isVECTOR()) {
        template = new Template(config.getTEMPLATE_NAME(), true);
      } else {
        template = new Template(config.getTEMPLATE_NAME(), false);
      }
      try {
        for (Sensor sensor : deviceSchema.getSensors()) {
          MeasurementNode measurementNode =
              new MeasurementNode(
                  sensor.getName(),
                  Enum.valueOf(TSDataType.class, sensor.getSensorType().name),
                  Enum.valueOf(TSEncoding.class, getEncodingType(sensor.getSensorType())),
                  Enum.valueOf(CompressionType.class, config.getCOMPRESSOR()));
          template.addToTemplate(measurementNode);
        }
      } catch (StatementExecutionException e) {
        LOGGER.error(e.getMessage());
        return null;
      }
    }
    return template;
  }

  /** register template */
  private void registerTemplate(Session metaSession, Template template)
      throws IoTDBConnectionException, IOException {
    try {
      metaSession.createSchemaTemplate(template);
    } catch (StatementExecutionException e) {
      // do nothing
      e.printStackTrace();
    }
  }

  private void registerStorageGroups(Session metaSession, List<TimeseriesSchema> schemaList)
      throws TsdbException {
    // get all storage groups
    Set<String> groups = new HashSet<>();
    for (TimeseriesSchema timeseriesSchema : schemaList) {
      DeviceSchema schema = timeseriesSchema.getDeviceSchema();
      synchronized (IoTDB.class) {
        if (!storageGroups.contains(schema.getGroup())) {
          groups.add(schema.getGroup());
          storageGroups.add(schema.getGroup());
        }
      }
    }
    // register storage groups
    for (String group : groups) {
      try {
        metaSession.setStorageGroup(ROOT_SERIES_NAME + "." + group);
        if (config.isTEMPLATE()) {
          metaSession.setSchemaTemplate(config.getTEMPLATE_NAME(), ROOT_SERIES_NAME + "." + group);
        }
      } catch (Exception e) {
        handleRegisterException(e);
      }
    }
  }

  private void activateTemplate(Session metaSession, List<TimeseriesSchema> schemaList) {
    List<String> someDevicePaths = new ArrayList<>();
    AtomicLong activatedDeviceCount = new AtomicLong();
    for (TimeseriesSchema timeseriesSchema : schemaList) {
      someDevicePaths.add(timeseriesSchema.getDeviceId());
      if (someDevicePaths.size() >= ACTIVATE_TEMPLATE_THRESHOLD) {
        activateTemplateForSomeDevices(metaSession, someDevicePaths, activatedDeviceCount.get());
        activatedDeviceCount.addAndGet(someDevicePaths.size());
        someDevicePaths.clear();
      }
    }
    if (!someDevicePaths.isEmpty()) {
      activateTemplateForSomeDevices(metaSession, someDevicePaths, activatedDeviceCount.get());
    }
  }

  private void activateTemplateForSomeDevices(
      Session metaSession, List<String> partialDevicePaths, long startIndex) {
    try {
      metaSession.createTimeseriesUsingSchemaTemplate(partialDevicePaths);
    } catch (Exception e) {
      LOGGER.error(
          "Activate {}~{} devices' schema template fail",
          startIndex,
          startIndex + partialDevicePaths.size(),
          e);
      System.exit(1);
    }
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
      tsEncodings.add(Enum.valueOf(TSEncoding.class, getEncodingType(datatype)));
      compressionTypes.add(Enum.valueOf(CompressionType.class, config.getCOMPRESSOR()));
    }
    TimeseriesSchema timeseriesSchema =
        new TimeseriesSchema(deviceSchema, paths, tsDataTypes, tsEncodings, compressionTypes);
    if (config.isVECTOR()) {
      timeseriesSchema.setDeviceId(getDevicePath(deviceSchema));
    }
    return timeseriesSchema;
  }

  private List<TimeseriesSchema> createTimeseries(List<DeviceSchema> schemaList) {
    List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
    for (DeviceSchema deviceSchema : schemaList) {
      TimeseriesSchema timeseriesSchema = createTimeseries(deviceSchema);
      timeseriesSchemas.add(timeseriesSchema);
    }
    return timeseriesSchemas;
  }

  private void registerTimeseries(Session metaSession, List<TimeseriesSchema> timeseriesSchemas)
      throws TsdbException {
    // create time series
    for (TimeseriesSchema timeseriesSchema : timeseriesSchemas) {
      try {
        if (config.isVECTOR()) {
          metaSession.createAlignedTimeseries(
              timeseriesSchema.getDeviceId(),
              timeseriesSchema.getPaths(),
              timeseriesSchema.getTsDataTypes(),
              timeseriesSchema.getTsEncodings(),
              timeseriesSchema.getCompressionTypes(),
              null);
        } else {
          metaSession.createMultiTimeseries(
              timeseriesSchema.getPaths(),
              timeseriesSchema.getTsDataTypes(),
              timeseriesSchema.getTsEncodings(),
              timeseriesSchema.getCompressionTypes(),
              null,
              null,
              null,
              null);
        }
      } catch (Exception e) {
        handleRegisterException(e);
      }
    }
  }

  private void handleRegisterException(Exception e) throws TsdbException {
    // ignore if already has the time series
    if (!e.getMessage().contains(ALREADY_KEYWORD) && !e.getMessage().contains("300")) {
      LOGGER.error("Register IoTDB schema failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public Status insertOneBatch(IBatch batch) throws DBConnectException {
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      for (Record record : batch.getRecords()) {
        String sql =
            getInsertOneBatchSql(
                batch.getDeviceSchema(), record.getTimestamp(), record.getRecordDataValue());
        statement.addBatch(sql);
      }
      statement.executeBatch();
      return new Status(true);
    } catch (Exception e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  /**
   * Q1: PreciseQuery SQL: select {sensors} from {devices} where time = {time}
   *
   * @param preciseQuery universal precise query condition parameters
   * @return
   */
  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    String strTime = preciseQuery.getTimestamp() + "";
    String sql = getSimpleQuerySqlHead(preciseQuery.getDeviceSchema()) + " WHERE time = " + strTime;
    return addTailClausesAndExecuteQueryAndGetStatus(sql, Operation.PRECISE_QUERY);
  }

  /**
   * Q2: RangeQuery SQL: select {sensors} from {devices} where time >= {startTime} and time <=
   * {endTime}
   *
   * @param rangeQuery universal range query condition parameters
   * @return
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    String sql =
        getRangeQuerySql(
            rangeQuery.getDeviceSchema(),
            rangeQuery.getStartTimestamp(),
            rangeQuery.getEndTimestamp());
    return addTailClausesAndExecuteQueryAndGetStatus(sql, Operation.RANGE_QUERY);
  }

  /**
   * Q3: ValueRangeQuery SQL: select {sensors} from {devices} where time >= {startTime} and time <=
   * {endTime} and {sensors} > {value}
   *
   * @param valueRangeQuery contains universal range query with value filter parameters
   * @return
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    String sql = getValueRangeQuerySql(valueRangeQuery);
    return addTailClausesAndExecuteQueryAndGetStatus(sql, Operation.VALUE_RANGE_QUERY);
  }

  /**
   * Q4: AggRangeQuery SQL: select {AggFun}({sensors}) from {devices} where time >= {startTime} and
   * time <= {endTime}
   *
   * @param aggRangeQuery contains universal aggregation query with time filter parameters
   * @return
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    String aggQuerySqlHead =
        getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(), aggRangeQuery.getAggFun());
    String sql =
        addWhereTimeClause(
            aggQuerySqlHead, aggRangeQuery.getStartTimestamp(), aggRangeQuery.getEndTimestamp());
    return addTailClausesAndExecuteQueryAndGetStatus(sql, Operation.AGG_RANGE_QUERY);
  }

  /**
   * Q5: AggValueQuery SQL: select {AggFun}({sensors}) from {devices} where {sensors} > {value}
   *
   * @param aggValueQuery contains universal aggregation query with value filter parameters
   * @return
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
    return addTailClausesAndExecuteQueryAndGetStatus(sql, Operation.AGG_VALUE_QUERY);
  }

  /**
   * Q6: AggRangeValueQuery SQL: select {AggFun}({sensors}) from {devices} where time >= {startTime}
   * and time <= {endTime} and {sensors} > {value}
   *
   * @param aggRangeValueQuery contains universal aggregation query with time and value filters
   *     parameters
   * @return
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
    return addTailClausesAndExecuteQueryAndGetStatus(sql, Operation.AGG_RANGE_VALUE_QUERY);
  }

  /**
   * Q7: GroupByQuery SQL: select {AggFun}({sensors}) from {devices} group by ([{start}, {end}],
   * {Granularity}ms)
   *
   * @param groupByQuery contains universal group by query condition parameters
   * @return
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
    return addTailClausesAndExecuteQueryAndGetStatus(sql, Operation.GROUP_BY_QUERY);
  }

  /**
   * Q8: LatestPointQuery SQL: select last {sensors} from {devices}
   *
   * @param latestPointQuery contains universal latest point query condition parameters
   * @return
   */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    String aggQuerySqlHead = getLatestPointQuerySql(latestPointQuery.getDeviceSchema());
    return addTailClausesAndExecuteQueryAndGetStatus(aggQuerySqlHead, Operation.LATEST_POINT_QUERY);
  }

  /**
   * Q9: RangeQuery SQL: select {sensors} from {devices} where time >= {startTime} and time <=
   * {endTime} order by time desc
   *
   * @param rangeQuery universal range query condition parameters
   * @return
   */
  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    String sql =
        getRangeQuerySql(
                rangeQuery.getDeviceSchema(),
                rangeQuery.getStartTimestamp(),
                rangeQuery.getEndTimestamp())
            + ORDER_BY_TIME_DESC;
    return addTailClausesAndExecuteQueryAndGetStatus(sql, Operation.RANGE_QUERY_ORDER_BY_TIME_DESC);
  }

  /**
   * Q10: ValueRangeQuery SQL: select {sensors} from {devices} where time >= {startTime} and time <=
   * {endTime} and {sensors} > {value} order by time desc
   *
   * @param valueRangeQuery contains universal range query with value filter parameters
   * @return
   */
  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    String sql = getValueRangeQuerySql(valueRangeQuery) + ORDER_BY_TIME_DESC;
    return addTailClausesAndExecuteQueryAndGetStatus(
        sql, Operation.VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC);
  }

  /** Q11: Q7 order by time desc */
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
    return addTailClausesAndExecuteQueryAndGetStatus(
        sql, Operation.GROUP_BY_QUERY_ORDER_BY_TIME_DESC);
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
   * @param devices
   * @param builder
   * @return From clause, e.g. FROM devices
   */
  private String addFromClause(List<DeviceSchema> devices, StringBuilder builder) {
    builder.append(" FROM ").append(getDevicePath(devices.get(0)));
    for (int i = 1; i < devices.size(); i++) {
      builder.append(", ").append(getDevicePath(devices.get(i)));
    }
    return builder.toString();
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
    for (Sensor sensor : deviceSchemas.get(0).getSensors()) {
      builder.append(" AND ").append(sensor.getName()).append(" > ").append(valueThreshold);
    }
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
    return addWhereTimeClause(getSimpleQuerySqlHead(deviceSchemas), start, end);
  }

  private String addWhereTimeClause(String prefix, long start, long end) {
    String startTime = start + "";
    String endTime = end + "";
    return prefix + " WHERE time >= " + startTime + " AND time <= " + endTime;
  }

  private String addGroupByClause(String prefix, long start, long end, long granularity) {
    return prefix + " group by ([" + start + "," + end + ")," + granularity + "ms) ";
  }

  /**
   * convert deviceSchema to the format
   *
   * @param deviceSchema
   * @return format, e.g. root.group_1.d_1
   */
  protected String getDevicePath(DeviceSchema deviceSchema) {
    StringBuilder name = new StringBuilder(ROOT_SERIES_NAME);
    name.append(".").append(deviceSchema.getGroup());
    for (Map.Entry<String, String> pair : deviceSchema.getTags().entrySet()) {
      name.append(".").append(pair.getValue());
    }
    name.append(".").append(deviceSchema.getDevice());
    return name.toString();
  }

  protected Status addTailClausesAndExecuteQueryAndGetStatus(String sql, Operation operation) {
    if (config.getRESULT_ROW_LIMIT() >= 0) {
      sql += " limit " + config.getRESULT_ROW_LIMIT();
    }
    if (config.isALIGN_BY_DEVICE()) {
      sql += " align by device";
    }
    String executeSQL;
    if (config.isIOTDB_USE_DEBUG() && random.nextDouble() < config.getIOTDB_USE_DEBUG_RATIO()) {
      executeSQL = "debug " + sql;
    } else {
      executeSQL = sql;
    }
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), executeSQL);
    }
    AtomicInteger line = new AtomicInteger();
    AtomicLong queryResultPointNum = new AtomicLong();
    AtomicBoolean isOk = new AtomicBoolean(true);
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      List<List<Object>> records = new ArrayList<>();
      task =
          service.submit(
              () -> {
                try {
                  try (ResultSet resultSet = statement.executeQuery(executeSQL)) {
                    while (resultSet.next()) {
                      line.getAndIncrement();
                      if (config.isIS_COMPARISON()) {
                        List<Object> record = new ArrayList<>();
                        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                          switch (operation) {
                            case LATEST_POINT_QUERY:
                              if (i == 2 || i >= 4) {
                                continue;
                              }
                              break;
                            default:
                              break;
                          }
                          record.add(resultSet.getObject(i));
                        }
                        records.add(record);
                      }
                    }
                  }
                } catch (SQLException e) {
                  LOGGER.error("exception occurred when execute query={}", executeSQL, e);
                  isOk.set(false);
                }
                long resultPointNum = line.get();
                if (!Operation.LATEST_POINT_QUERY.equals(operation)
                    || !config.isALIGN_BY_DEVICE()) {
                  resultPointNum *= config.getQUERY_SENSOR_NUM();
                  resultPointNum *= config.getQUERY_DEVICE_NUM();
                }
                queryResultPointNum.set(resultPointNum);
              });
      try {
        task.get(config.getREAD_OPERATION_TIMEOUT_MS(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        task.cancel(true);
        return new Status(false, queryResultPointNum.get(), e, executeSQL);
      }
      if (isOk.get() == true) {
        if (config.isIS_COMPARISON()) {
          return new Status(true, queryResultPointNum.get(), executeSQL, records);
        } else {
          return new Status(true, queryResultPointNum.get());
        }
      } else {
        return new Status(
            false, queryResultPointNum.get(), new Exception("Failed to execute."), executeSQL);
      }
    } catch (Exception e) {
      return new Status(false, queryResultPointNum.get(), e, executeSQL);
    } catch (Throwable t) {
      return new Status(false, queryResultPointNum.get(), new Exception(t), executeSQL);
    }
  }

  public String getInsertOneBatchSql(
      DeviceSchema deviceSchema, long timestamp, List<Object> values) {
    StringBuilder builder = new StringBuilder("insert into ");
    builder.append(getDevicePath(deviceSchema)).append("(timestamp");
    for (Sensor sensor : deviceSchema.getSensors()) {
      builder.append(",").append(sensor.getName());
    }
    if (config.isVECTOR() == true) {
      builder.append(") aligned values(");
    } else {
      builder.append(") values(");
    }
    builder.append(timestamp);
    int sensorIndex = 0;
    List<Sensor> sensors = deviceSchema.getSensors();
    for (Object value : values) {
      switch (sensors.get(sensorIndex).getSensorType()) {
        case BOOLEAN:
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
          builder.append(",").append(value);
          break;
        case TEXT:
          builder.append(",").append("'").append(value).append("'");
          break;
      }
      sensorIndex++;
    }
    builder.append(")");
    LOGGER.debug("getInsertOneBatchSql: {}", builder);
    return builder.toString();
  }

  /**
   * Using in verification
   *
   * @param verificationQuery
   */
  @Override
  public Status verificationQuery(VerificationQuery verificationQuery) {
    DeviceSchema deviceSchema = verificationQuery.getDeviceSchema();
    List<DeviceSchema> deviceSchemas = new ArrayList<>();
    deviceSchemas.add(deviceSchema);

    List<Record> records = verificationQuery.getRecords();
    if (records == null || records.size() == 0) {
      return new Status(
          false,
          new TsdbException("There are no records in verficationQuery."),
          "There are no records in verficationQuery.");
    }

    StringBuffer sql = new StringBuffer();
    sql.append(getSimpleQuerySqlHead(deviceSchemas));
    Map<Long, List<Object>> recordMap = new HashMap<>();
    sql.append(" WHERE time = ").append(records.get(0).getTimestamp());
    recordMap.put(records.get(0).getTimestamp(), records.get(0).getRecordDataValue());
    for (int i = 1; i < records.size(); i++) {
      Record record = records.get(i);
      sql.append(" or time = ").append(record.getTimestamp());
      recordMap.put(record.getTimestamp(), record.getRecordDataValue());
    }
    int point = 0;
    int line = 0;
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql.toString());
      while (resultSet.next()) {
        long timeStamp = resultSet.getLong(1);
        List<Object> values = recordMap.get(timeStamp);
        for (int i = 0; i < values.size(); i++) {
          String value = resultSet.getString(i + 2);
          String target = String.valueOf(values.get(i));
          if (!value.equals(target)) {
            LOGGER.error("Using SQL: " + sql + ",Expected:" + value + " but was: " + target);
          } else {
            point++;
          }
        }
        line++;
      }
    } catch (Exception e) {
      LOGGER.error("Query Error: " + sql);
      return new Status(false, new TsdbException("Failed to query"), "Failed to query.");
    }
    if (recordMap.size() != line) {
      LOGGER.error(
          "Using SQL: " + sql + ",Expected line:" + recordMap.size() + " but was: " + line);
    }
    return new Status(true, point);
  }

  @Override
  public Status deviceQuery(DeviceQuery deviceQuery) throws SQLException, TsdbException {
    DeviceSchema deviceSchema = deviceQuery.getDeviceSchema();
    String sql =
        getDeviceQuerySql(
            deviceSchema, deviceQuery.getStartTimestamp(), deviceQuery.getEndTimestamp());
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("IoTDB:" + sql);
    }
    List<List<Object>> result = new ArrayList<>();
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int colNumber = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        List<Object> line = new ArrayList<>();
        for (int i = 1; i <= colNumber; i++) {
          line.add(resultSet.getObject(i));
        }
        result.add(line);
      }
    } catch (Exception e) {
      LOGGER.error("Query Error: " + sql + " exception:" + e.getMessage());
      return new Status(false, new TsdbException("Failed to query"), "Failed to query.");
    }

    return new Status(true, 0, sql.toString(), result);
  }

  protected String getDeviceQuerySql(
      DeviceSchema deviceSchema, long startTimeStamp, long endTimeStamp) {
    StringBuffer sql = new StringBuffer();
    List<DeviceSchema> deviceSchemas = new ArrayList<>();
    deviceSchemas.add(deviceSchema);
    sql.append(getSimpleQuerySqlHead(deviceSchemas));
    sql.append(" where time >= ").append(startTimeStamp);
    sql.append(" and time <").append(endTimeStamp);
    sql.append(ORDER_BY_TIME_DESC);
    return sql.toString();
  }

  @Override
  public DeviceSummary deviceSummary(DeviceQuery deviceQuery) throws SQLException, TsdbException {
    DeviceSchema deviceSchema = deviceQuery.getDeviceSchema();
    int totalLineNumber = 0;
    long minTimeStamp = 0, maxTimeStamp = 0;
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      ResultSet resultSet = statement.executeQuery(getTotalLineNumberSql(deviceSchema));
      resultSet.next();
      totalLineNumber = Integer.parseInt(resultSet.getString(1));

      resultSet = statement.executeQuery(getMaxTimeStampSql(deviceSchema));
      resultSet.next();
      maxTimeStamp = Long.parseLong(resultSet.getObject(1).toString());

      resultSet = statement.executeQuery(getMinTimeStampSql(deviceSchema));
      resultSet.next();
      minTimeStamp = Long.parseLong(resultSet.getObject(1).toString());
    }
    return new DeviceSummary(deviceSchema.getDevice(), totalLineNumber, minTimeStamp, maxTimeStamp);
  }

  protected String getTotalLineNumberSql(DeviceSchema deviceSchema) {
    return "select count(*) from " + getDevicePath(deviceSchema);
  }

  protected String getMinTimeStampSql(DeviceSchema deviceSchema) {
    return "select * from " + getDevicePath(deviceSchema) + " order by time limit 1";
  }

  protected String getMaxTimeStampSql(DeviceSchema deviceSchema) {
    return "select * from " + getDevicePath(deviceSchema) + ORDER_BY_TIME_DESC + " limit 1";
  }

  String getEncodingType(SensorType dataSensorType) {
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
      default:
        LOGGER.error("Unsupported data sensorType {}.", dataSensorType);
        return null;
    }
  }

  /**
   * convert deviceSchema and sensor to the format: root.group_1.d_1.s_1
   *
   * @param deviceSchema
   * @param sensor
   * @return
   */
  private String getSensorPath(DeviceSchema deviceSchema, String sensor) {
    return getDevicePath(deviceSchema) + "." + sensor;
  }
}
