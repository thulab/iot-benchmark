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
import org.apache.iotdb.session.Session;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.iotdb200.IoTDB;
import cn.edu.tsinghua.iot.benchmark.iotdb200.TimeseriesSchema;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class IoTDBModelStrategy {
  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected final DBConfig dbConfig;
  protected static String ROOT_SERIES_NAME;
  protected static int queryBaseOffset;
  protected static final Set<String> databases = new HashSet<>();

  public IoTDBModelStrategy(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  public abstract void registerSchema(
      Map<Session, List<TimeseriesSchema>> sessionListMap, List<DeviceSchema> schemaList)
      throws TsdbException;

  // region select

  public abstract String selectTimeColumnIfNecessary();

  public abstract String addFromClause(List<DeviceSchema> devices, StringBuilder builder);

  public abstract String addDeviceIDColumnIfNecessary(List<DeviceSchema> deviceSchemas, String sql);

  public abstract void deleteIDColumnIfNecessary(
      List<Tablet.ColumnType> columnTypes, List<Sensor> sensors, IBatch batch);

  public abstract void addVerificationQueryWhereClause(
      StringBuffer sql,
      List<Record> records,
      Map<Long, List<Object>> recordMap,
      DeviceSchema deviceSchema);

  public abstract void getValueFilterClause(
      List<DeviceSchema> deviceSchemas, int valueThreshold, StringBuilder builder);

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

  public abstract void registerDatabases(Session metaSession, List<TimeseriesSchema> schemaList)
      throws TsdbException;

  public abstract Tablet createTablet(
      String insertTargetName,
      List<IMeasurementSchema> schemas,
      List<Tablet.ColumnType> columnTypes,
      int maxRowNumber);

  public abstract String getInsertTargetName(DeviceSchema schema);

  public abstract void addIDColumnIfNecessary(
      List<Tablet.ColumnType> columnTypes, List<Sensor> sensors, IBatch batch);

  public abstract void sessionInsertImpl(Session session, Tablet tablet, DeviceSchema deviceSchema)
      throws IoTDBConnectionException, StatementExecutionException;

  public abstract void sessionCleanupImpl(Session session)
      throws IoTDBConnectionException, StatementExecutionException;

  // endregion

  public abstract Logger getLogger();

  public void handleRegisterException(Exception e) throws TsdbException {
    // ignore if already has the time series
    if (!e.getMessage().contains(IoTDB.ALREADY_KEYWORD) && !e.getMessage().contains("300")) {
      Logger LOGGER = getLogger();
      LOGGER.error("Register IoTDB schema failed because ", e);
      throw new TsdbException(e);
    }
  }
}
