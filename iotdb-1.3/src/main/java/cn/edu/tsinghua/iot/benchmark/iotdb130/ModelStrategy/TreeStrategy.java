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

import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.MeasurementNode;

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

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class TreeStrategy extends IoTDBModelStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(TreeStrategy.class);
  private final IoTDB iotdb;
  private final Random random = new Random(config.getDATA_SEED());
  private static final Set<String> storageGroups = Collections.synchronizedSet(new HashSet<>());
  private static final CyclicBarrier templateBarrier =
      new CyclicBarrier(config.getCLIENT_NUMBER());
  private static final CyclicBarrier schemaBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());
  private static final CyclicBarrier activateTemplateBarrier =
      new CyclicBarrier(config.getCLIENT_NUMBER());
  private static final AtomicBoolean templateInit = new AtomicBoolean(false);

  public TreeStrategy(DBConfig dbConfig, IoTDB iotdb) {
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
        .version(Version.V_1_0)
        .sqlDialect(dbConfig.getSQL_DIALECT())
        .build();
  }

  @Override
  public Double registerSchema(
      Map<Session, List<TimeseriesSchema>> sessionListMap, List<DeviceSchema> schemaList)
      throws TsdbException {
    long start = System.nanoTime();
    long end;
    try {
      if (config.isTEMPLATE()
          && !config.isIoTDB_ENABLE_TABLE()
          && templateInit.compareAndSet(false, true)) {
        Template template = null;
        if (config.isTEMPLATE() && schemaList.size() > 0) {
          template = createTemplate(schemaList.get(0));
        }
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
          registerTimeSeries(pair.getKey(), pair.getValue());
        }
      }
    } catch (Exception e) {
      throw new TsdbException(e);
    }
    end = System.nanoTime();
    return TimeUtils.convertToSeconds(end - start, "ns");
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
        metaSession.setStorageGroup(iotdb.ROOT_SERIES_NAME + "." + group);
        if (config.isTEMPLATE()) {
          metaSession.setSchemaTemplate(
              config.getTEMPLATE_NAME(), iotdb.ROOT_SERIES_NAME + "." + group);
        }
      } catch (Exception e) {
        handleRegisterException(e);
      }
    }
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
                  Enum.valueOf(
                      TSEncoding.class,
                      Objects.requireNonNull(IoTDB.getEncodingType(sensor.getSensorType()))),
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

  private void activateTemplate(Session metaSession, List<TimeseriesSchema> schemaList) {
    try {
      List<String> devicePaths =
          schemaList.stream()
              .map(
                  schema -> iotdb.ROOT_SERIES_NAME + "." + schema.getDeviceSchema().getDevicePath())
              .collect(Collectors.toList());
      metaSession.createTimeseriesUsingSchemaTemplate(devicePaths);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  @Override
  public void registerDatabase(Map<Session, List<TimeseriesSchema>> sessionListMap)
      throws TsdbException {
    Set<String> storageGroups = Collections.synchronizedSet(new HashSet<>());
    for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
      Session metaSession = pair.getKey();
      List<TimeseriesSchema> schemaList = pair.getValue();
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
          metaSession.setStorageGroup(iotdb.ROOT_SERIES_NAME + "." + group);
          if (config.isTEMPLATE()) {
            metaSession.setSchemaTemplate(
                config.getTEMPLATE_NAME(), iotdb.ROOT_SERIES_NAME + "." + group);
          }
        } catch (Exception e) {
          handleRegisterException(e);
        }
      }
    }
  }

  @Override
  public void registerTimeSeries(Session metaSession, List<TimeseriesSchema> timeseriesSchemas)
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

  @Override
  public Tablet createTablet(
      String insertTargetName,
      List<IMeasurementSchema> schemas,
      List<Tablet.ColumnType> columnTypes,
      int maxRowNumber) {
    return new Tablet(insertTargetName, schemas, maxRowNumber);
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
      timeseriesSchema.setDeviceId(getDevicePath(deviceSchema));
    }
    return timeseriesSchema;
  }

  @Override
  public String getDeviceId(DeviceSchema schema) {
    return getDevicePath(schema);
  }

  @Override
  public StringBuilder getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<Sensor> querySensors = devices.get(0).getSensors();
    builder.append(querySensors.get(0).getName());
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
        builder.append(" AND ").append(sensor.getName()).append(" > ");
        if (sensor.getSensorType() == SensorType.DATE) {
          builder.append("'").append(LocalDate.ofEpochDay(Math.abs(valueThreshold))).append("'");
        } else {
          builder.append(valueThreshold);
        }
      }
    }
    return builder.toString();
  }

  @Override
  public String addFromClause(List<DeviceSchema> devices, StringBuilder builder) {
    // The time series of the tree model is mapped to the table model. In "root.test.g_0.d_0.s_0",
    // test is the database name, g_0_table is the table name, and the device is the identification
    // column.
    builder.append(" FROM ").append(getDevicePath(devices.get(0)));
    for (int i = 1; i < devices.size(); i++) {
      builder.append("." + getDevicePath(devices.get(i)));
    }
    return builder.toString();
  }

  @Override
  public void sessionInsertImpl(Session session, Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    if (config.isVECTOR()) {
      session.insertAlignedTablet(tablet);
    } else {
      session.insertTablet(tablet);
    }
  }

  @Override
  public void sessionCleanupImpl(Session session)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.executeNonQueryStatement(
          "drop database root." + config.getDbConfig().getDB_NAME() + ".**");
      session.executeNonQueryStatement("drop schema template " + config.getTEMPLATE_NAME());
    } catch (IoTDBConnectionException e) {
      LOGGER.warn("Failed to connect to IoTDB:" + e.getMessage());
    } catch (StatementExecutionException e) {
      LOGGER.warn("Failed to execute statement:" + e.getMessage());
    }
  }

  /**
   * convert deviceSchema to the format
   *
   * @param deviceSchema
   * @return format, e.g. root.group_1.d_1
   */
  protected String getDevicePath(DeviceSchema deviceSchema) {
    // TODO: copy from IoTDB.getDevicePath, should think a better way
    StringBuilder name = new StringBuilder(iotdb.ROOT_SERIES_NAME);
    name.append(".").append(deviceSchema.getGroup());
    for (Map.Entry<String, String> pair : deviceSchema.getTags().entrySet()) {
      name.append(".").append(pair.getValue());
    }
    name.append(".").append(deviceSchema.getDevice());
    return name.toString();
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

  private void handleRegisterException(Exception e) throws TsdbException {
    // ignore if already has the time series
    if (!e.getMessage().contains(IoTDB.ALREADY_KEYWORD) && !e.getMessage().contains("300")) {
      LOGGER.error("Register IoTDB schema failed because ", e);
      throw new TsdbException(e);
    }
  }
}
