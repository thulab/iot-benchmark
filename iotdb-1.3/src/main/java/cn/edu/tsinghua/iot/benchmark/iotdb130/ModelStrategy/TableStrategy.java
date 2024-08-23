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

import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.iotdb130.IoTDB;
import cn.edu.tsinghua.iot.benchmark.iotdb130.TimeseriesSchema;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

public class TableStrategy extends IoTDBModelStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableStrategy.class);
//  private static final AtomicBoolean databaseNotExist = new AtomicBoolean(true);
  private static boolean databaseCreated = false;
  private static final CyclicBarrier schemaBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());

  private final IoTDB iotdb;

  public TableStrategy(DBConfig dbConfig, IoTDB iotdb) {
    super(dbConfig);
    this.iotdb = iotdb;
  }

  @Override
  public Session buildSession(List<String> hostUrls) {
    return new Session.Builder()
        .nodeUrls(hostUrls)
        .username(dbConfig.getUSERNAME())
        .password(dbConfig.getPASSWORD())
        .enableRedirection(true)
        .database(dbConfig.getDB_NAME())
        .version(Version.V_1_0)
        .sqlDialect(dbConfig.getSQL_DIALECT())
        .build();
  }

  @Override
  public Double registerSchema(
      Map<Session, List<TimeseriesSchema>> sessionListMap, List<DeviceSchema> schemaList)
      throws TsdbException {
    long start;
    long end;
    try {
      start = System.nanoTime();
      registerDatabase(sessionListMap);
      schemaBarrier.await();
      for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
        registerTable(pair.getKey(), pair.getValue());
      }
    } catch (Exception e) {
      throw new TsdbException(e);
    }
    end = System.nanoTime();
    return TimeUtils.convertToSeconds(end - start, "ns");
  }

  /**
   * root.test.g_0.d_0 test is the database name.Ensure that only one client creates the table.
   *
   * @param sessionListMap
   */
  @Override // TODO：非override
  public void registerDatabase(Map<Session, List<TimeseriesSchema>> sessionListMap) {
    Session session = sessionListMap.keySet().iterator().next();
    if (!databaseCreated) {
      synchronized (TableStrategy.class) {
        if (!databaseCreated) {
          try {
            session.executeNonQueryStatement(
                "create database " + config.getDbConfig().getDB_NAME());
            databaseCreated = true;
          } catch (IoTDBConnectionException | StatementExecutionException e) {
            LOGGER.error("Failed to create database:" + e.getMessage());
          }
        }
      }
    }
  }

  /**
   * Verify that the database not exists.
   *
   * @param metaSession
   * @return
   */
//  private static AtomicBoolean isDatabaseNotExist(Session metaSession) {
//    try {
//      SessionDataSet dataSet = metaSession.executeQueryStatement("show databases");
//      while (dataSet.hasNext()) {
//        if (dataSet
//            .next()
//            .getFields()
//            .get(0)
//            .toString()
//            .equals(config.getDbConfig().getDB_NAME())) {
//          databaseNotExist.set(false);
//          return databaseNotExist;
//        }
//      }
//    } catch (IoTDBConnectionException | StatementExecutionException e) {
//      LOGGER.error("Failed to show database:" + e.getMessage());
//    }
//    databaseNotExist.set(true);
//    return databaseNotExist;
//  }

  @Override
  public void registerTimeSeries(Session metaSession, List<TimeseriesSchema> timeseriesSchemas)// TODO：删
      throws TsdbException {
    // nothing
  }

  private void registerTable(Session metaSession, List<TimeseriesSchema> timeseriesSchemas)
      throws TsdbException {
    try {
      DeviceSchema deviceSchema = timeseriesSchemas.get(0).getDeviceSchema();
      StringBuilder builder = new StringBuilder();
      // g_0_table is the database name
      builder
          .append("create table if not exists ")
          .append(deviceSchema.getGroup())
          .append("_table(");
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
      metaSession.executeNonQueryStatement("use " + config.getDbConfig().getDB_NAME());
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
    return schema.getGroup() + "_table";
  }

  @Override
  public StringBuilder getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<Sensor> querySensors = devices.get(0).getSensors();
    builder.append(" time, ").append(querySensors.get(0).getName()); // TODO: 抽的更细？
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i).getName());
    }
    return builder;
  }

  @Override
  public String getValueFilterClause(List<DeviceSchema> deviceSchemas, int valueThreshold) {
    StringBuilder builder = new StringBuilder();
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        String devicePath = getDevicePath(deviceSchema) + ".";
        builder.append(" AND ").append(devicePath).append(sensor.getName()).append(" > "); // TODO：devicePath 树模型表模型统一？
        if (sensor.getSensorType() == SensorType.DATE) {
          builder.append("'").append(LocalDate.ofEpochDay(Math.abs(valueThreshold))).append("'");
        } else {
          builder.append(valueThreshold);
        }
      }
    }
    return builder.toString();
  }

  private String getDevicePath(DeviceSchema deviceSchema) {
    StringBuilder name = new StringBuilder(iotdb.ROOT_SERIES_NAME);
    name.append(".").append(deviceSchema.getGroup());
    for (Map.Entry<String, String> pair : deviceSchema.getTags().entrySet()) {
      name.append(".").append(pair.getValue());
    }
    name.append(".").append(deviceSchema.getDevice());
    return name.toString();
  }

  @Override
  public String addFromClause(List<DeviceSchema> devices, StringBuilder builder) {
    // The time series of the tree model is mapped to the table model. In "root.test.g_0.d_0.s_0",
    // test is the database name, g_0_table is the table name, and the device is the identification
    // column.
    builder.append(" FROM ").append(devices.get(0).getGroup() + "_table");

    for (int i = 1; i < devices.size(); i++) {
      builder.append(devices.get(i).getGroup() + "_table");
    }
    return builder.toString();
  }

  @Override
  public void sessionInsertImpl(Session session, Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    session.insertRelationalTablet(tablet);
  }

  @Override
  public void sessionCleanupImpl(Session session)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement("show databases");
    while (dataSet.hasNext()) {
      //      System.out.println(dataSet.next().getFields().get(0).toString());
      session.executeNonQueryStatement(
          "drop database " + dataSet.next().getFields().get(0).toString());
    }
  }

  @Override
  public List<TimeseriesSchema> createTimeseries(List<DeviceSchema> schemaList) {
    List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
    for (DeviceSchema deviceSchema : schemaList) {
      TimeseriesSchema timeseriesSchema = createTimeseries(deviceSchema);
      timeseriesSchemas.add(timeseriesSchema);
    }
    return timeseriesSchemas;
  }

  private TimeseriesSchema createTimeseries(DeviceSchema deviceSchema) { // TODO：抽
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
      timeseriesSchema.setDeviceId(getDevicePath(deviceSchema));
    }
    return timeseriesSchema;
  }

  private String getSensorPath(DeviceSchema deviceSchema, String sensor) {
    return getDevicePath(deviceSchema) + "." + sensor;
  }

  private void handleRegisterException(Exception e) throws TsdbException {
    // ignore if already has the time series
    if (!e.getMessage().contains(IoTDB.ALREADY_KEYWORD) && !e.getMessage().contains("300")) {
      LOGGER.error("Register IoTDB schema failed because ", e);
      throw new TsdbException(e);
    }
  }
}
