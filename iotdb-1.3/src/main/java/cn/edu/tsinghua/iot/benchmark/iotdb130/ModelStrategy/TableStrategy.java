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

package cn.edu.tsinghua.iot.benchmark.iotdb130.ModelStrategy;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.iotdb130.IoTDB;
import cn.edu.tsinghua.iot.benchmark.iotdb130.TimeseriesSchema;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

// import java.util.concurrent.atomic.AtomicBoolean;

public class TableStrategy extends IoTDBModelStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableStrategy.class);
  private static final Set<String> databases = Collections.synchronizedSet(new HashSet<>());
  //  private static boolean databaseCreated = false;
  private static final CyclicBarrier schemaBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());
  private static String ROOT_SERIES_NAME;

  public TableStrategy(DBConfig dbConfig, String ROOT_SERIES_NAME) {
    super(dbConfig);
    TableStrategy.ROOT_SERIES_NAME = ROOT_SERIES_NAME;
  }

  @Override
  public Session buildSession(List<String> hostUrls) {
    return new Session.Builder()
        .nodeUrls(hostUrls)
        .username(dbConfig.getUSERNAME())
        .password(dbConfig.getPASSWORD())
        .enableRedirection(true)
        //        .database(dbConfig.getDB_NAME())
        .version(Version.V_1_0)
        .sqlDialect(dbConfig.getSQL_DIALECT())
        .build();
  }

  @Override
  public void registerSchema(
      Map<Session, List<TimeseriesSchema>> sessionListMap, List<DeviceSchema> schemaList)
      throws TsdbException {
    try {
      // TODO 多个 database
      for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
        registerDatabase(pair.getKey(), pair.getValue());
      }
      //      registerDatabase(sessionListMap);
      schemaBarrier.await();
      for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
        registerTable(pair.getKey(), pair.getValue());
      }
    } catch (Exception e) {
      throw new TsdbException(e);
    }
  }

  /** root.test.g_0.d_0 test is the database name.Ensure that only one client creates the table. */
  public void registerDatabase(Session metaSession, List<TimeseriesSchema> schemaList)
      throws TsdbException {
    Set<String> groups = new HashSet<>();
    for (TimeseriesSchema timeseriesSchema : schemaList) {
      DeviceSchema schema = timeseriesSchema.getDeviceSchema();
      synchronized (IoTDB.class) {
        if (!databases.contains(schema.getGroup())) {
          groups.add(schema.getGroup());
          databases.add(schema.getGroup());
        }
      }
    }
    // register storage groups
    for (String group : groups) {
      try {
        metaSession.executeNonQueryStatement(
            "CREATE DATABASE " + dbConfig.getDB_NAME() + "_" + group);
      } catch (Exception e) {
        handleRegisterException(e);
      }
    }
  }

  private void registerTable(Session metaSession, List<TimeseriesSchema> timeseriesSchemas)
      throws TsdbException {
    try {
      DeviceSchema deviceSchema = timeseriesSchemas.get(0).getDeviceSchema();
      StringBuilder builder = new StringBuilder();
      // g_0_table is the database name
      builder.append("create table if not exists ").append(deviceSchema.getTable()).append("(");
      for (int i = 0; i < deviceSchema.getSensors().size(); i++) {
        if (i != 0) builder.append(", ");
        builder
            .append(deviceSchema.getSensors().get(i).getName())
            .append(" ")
            .append(deviceSchema.getSensors().get(i).getSensorType())
            .append(" ")
            .append(deviceSchema.getSensors().get(i).getColumnCategory());
      }
      builder.append(")");
      //      System.out.println("use " + dbConfig.getDB_NAME() + "_" +deviceSchema.getGroup());
      metaSession.executeNonQueryStatement(
          "use " + dbConfig.getDB_NAME() + "_" + deviceSchema.getGroup());
      //      System.out.println(builder.toString());
      metaSession.executeNonQueryStatement(builder.toString());
    } catch (Exception e) {
      handleRegisterException(e);
    }
  }

  @Override
  public Tablet createTablet(
      String insertTargetName,
      List<IMeasurementSchema> schemas,
      List<Tablet.ColumnType> columnTypes,
      int maxRowNumber) {
    return new Tablet(insertTargetName, schemas, columnTypes, maxRowNumber);
  }

  @Override
  public String getDeviceId(DeviceSchema schema) {
    return schema.getTable();
  }

  @Override
  public String addSelectClause() {
    return " time, ";
  }

  @Override
  public String addPath(DeviceSchema deviceSchema) {
    return "";
  }

  @Override
  public String addFromClause(List<DeviceSchema> devices, StringBuilder builder) {
    // The time series of the tree model is mapped to the table model. In "root.test.g_0.d_0.s_0",
    // test is the database name, g_0_table is the table name, and the device is the identification
    // column.
    builder.append(" FROM ").append(devices.get(0).getGroup()).append("_table");

    for (int i = 1; i < devices.size(); i++) {
      builder.append(devices.get(i).getGroup() + "_table");
    }
    return builder.toString();
  }

  @Override
  public void sessionInsertImpl(Session session, Tablet tablet, DeviceSchema deviceSchema)
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(
        "use " + dbConfig.getDB_NAME() + "_" + deviceSchema.getGroup());
    session.insertRelationalTablet(tablet);
  }

  @Override
  public void sessionCleanupImpl(Session session)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement("show databases");
    while (dataSet.hasNext()) {
      String databaseName = dataSet.next().getFields().get(0).toString();
      //      System.out.println(databaseName);
      session.executeNonQueryStatement("drop database " + databaseName);
    }
  }

  @Override
  public void genTablet(List<Tablet.ColumnType> columnTypes, List<Sensor> sensors, IBatch batch) {
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
    for (int i = 0; i < batch.getRecords().size(); i++) {
      List<Object> dataValue = batch.getRecords().get(i).getRecordDataValue();
      dataValue.add(batch.getDeviceSchema().getDevice());
      for (String key : batch.getDeviceSchema().getTags().keySet()) {
        dataValue.add(batch.getDeviceSchema().getTags().get(key));
      }
    }
  }

  private void handleRegisterException(Exception e) throws TsdbException {
    // ignore if already has the time series
    if (!e.getMessage().contains(IoTDB.ALREADY_KEYWORD) && !e.getMessage().contains("300")) {
      LOGGER.error("Register IoTDB schema failed because ", e);
      throw new TsdbException(e);
    }
  }
}
