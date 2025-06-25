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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy.SessionManager;
import cn.edu.tsinghua.iot.benchmark.iotdb200.IoTDB;
import cn.edu.tsinghua.iot.benchmark.iotdb200.TimeseriesSchema;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

public abstract class IoTDBModelStrategy {
  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected final DBConfig dbConfig;
  protected static String ROOT_SERIES_NAME;
  protected static int queryBaseOffset;
  protected static final Set<String> databases = new HashSet<>();
  protected static final CyclicBarrier schemaBarrier =
      new CyclicBarrier(config.getSCHEMA_CLIENT_NUMBER());

  public IoTDBModelStrategy(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  public abstract void registerSchema(
      Map<SessionManager, List<TimeseriesSchema>> sessionListMap, List<DeviceSchema> schemaList)
      throws TsdbException;

  // region select
  public abstract String selectTimeColumnIfNecessary();

  public abstract String getAggQuerySqlHead(List<DeviceSchema> devices, String aggFun);

  public abstract String getGroupByQuerySQL(GroupByQuery groupByQuery, boolean addOrderBy);

  public abstract String getLatestPointQuerySql(List<DeviceSchema> devices);

  public abstract void addFromClause(List<DeviceSchema> devices, StringBuilder builder);

  public abstract void addOrderByTimeDesc(StringBuilder builder);

  public abstract void addPreciseQueryWhereClause(
      String strTime, List<DeviceSchema> deviceSchemas, StringBuilder builder);

  public abstract void addWhereClause(
      boolean addTime,
      boolean addValue,
      long start,
      long end,
      List<DeviceSchema> deviceSchemas,
      int valueThreshold,
      StringBuilder builder);

  public abstract void addAggWhereClause(
      boolean addTime,
      boolean addValue,
      long start,
      long end,
      List<DeviceSchema> deviceSchemas,
      int valueThreshold,
      StringBuilder builder);

  public abstract void addWhereValueClauseIfNecessary(
      List<DeviceSchema> devices, StringBuilder builder);

  public abstract String addGroupByClauseIfNecessary(String sql);

  public abstract void addVerificationQueryWhereClause(
      StringBuffer sql,
      List<Record> records,
      Map<Long, List<Object>> recordMap,
      DeviceSchema deviceSchema);

  public abstract void deleteIDColumnIfNecessary(
      List<ColumnCategory> columnTypes, List<Sensor> sensors, IBatch batch);

  public abstract String getValueFilterClause(List<DeviceSchema> deviceSchemas, int valueThreshold);

  public abstract long getTimestamp(RowRecord rowRecord);

  public abstract int getQueryOffset();

  public abstract String getTotalLineNumberSql(DeviceSchema deviceSchema);

  public abstract String getMaxTimeStampSql(DeviceSchema deviceSchema);

  public abstract String getMinTimeStampSql(DeviceSchema deviceSchema);

  // endregion

  // region insert

  public Set<String> getAllDataBase(List<TimeseriesSchema> schemaList) {
    Set<String> databaseNames = new HashSet<>();
    for (TimeseriesSchema timeseriesSchema : schemaList) {
      DeviceSchema schema = timeseriesSchema.getDeviceSchema();
      synchronized (IoTDB.class) {
        if (!databases.contains(schema.getGroup())) {
          databaseNames.add(schema.getGroup());
          databases.add(schema.getGroup());
        }
      }
    }
    return databaseNames;
  }

  public abstract void registerDatabases(
      SessionManager metaSession, List<TimeseriesSchema> schemaList) throws TsdbException;

  public abstract Tablet createTablet(
      String insertTargetName,
      List<IMeasurementSchema> schemas,
      List<ColumnCategory> columnTypes,
      int maxRowNumber);

  public abstract String getInsertTargetName(DeviceSchema schema);

  public abstract void addIDColumnIfNecessary(
      List<ColumnCategory> columnTypes, List<Sensor> sensors, IBatch batch);

  public abstract void sessionInsertImpl(
      SessionManager sessionManager, Tablet tablet, DeviceSchema deviceSchema)
      throws IoTDBConnectionException, StatementExecutionException;

  public abstract void sessionCleanupImpl(SessionManager sessionManager)
      throws IoTDBConnectionException, StatementExecutionException;

  // endregion

  public abstract Logger getLogger();

  /**
   * </> DESC
   *
   * <p>Table model The builder has already concatenated "SELECT device_id, date_bin(20000ms, time),
   * ".
   *
   * <p>Tree model The builder has already concatenated "SELECT ".
   *
   * <p>Therefore, the first loop does not need ", "
   */
  protected String getAggFunForGroupByQuery(List<Sensor> querySensors, String aggFunction) {
    StringBuilder builder = new StringBuilder();
    String timeArg = getTimeArg(aggFunction);
    for (int i = 0; i < querySensors.size(); i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder
          .append(aggFunction)
          .append("(")
          .append(timeArg)
          .append(querySensors.get(i).getName())
          .append(")");
    }
    return builder.toString();
  }

  protected abstract String getTimeArg(String aggFunction);

  protected String getTimeWhereClause(long start, long end) {
    StringBuilder builder = new StringBuilder();
    builder
        .append(" time >= ")
        .append(String.valueOf(start))
        .append(" AND time <= ")
        .append(String.valueOf(end));
    return builder.toString();
  }

  public void handleRegisterException(Exception e) throws TsdbException {
    // ignore if already has the time series
    if (!e.getMessage().contains(IoTDB.ALREADY_KEYWORD) && !e.getMessage().contains("300")) {
      Logger LOGGER = getLogger();
      LOGGER.error("Register IoTDB schema failed because ", e);
      throw new TsdbException(e);
    }
  }
}
