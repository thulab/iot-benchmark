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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.MeasurementNode;

import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.iotdb130.IoTDB;
import cn.edu.tsinghua.iot.benchmark.iotdb130.TimeseriesSchema;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
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

  private final Random random = new Random(config.getDATA_SEED());
  private static final CyclicBarrier templateBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());
  private static final CyclicBarrier schemaBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());
  private static final CyclicBarrier activateTemplateBarrier =
      new CyclicBarrier(config.getCLIENT_NUMBER());
  private static final AtomicBoolean templateInit = new AtomicBoolean(false);

  public TreeStrategy(DBConfig dbConfig) {
    super(dbConfig);
    ROOT_SERIES_NAME = IoTDB.ROOT_SERIES_NAME;
    queryBaseOffset = 0;
  }

  @Override
  public void registerSchema(
      Map<Session, List<TimeseriesSchema>> sessionListMap, List<DeviceSchema> schemaList)
      throws TsdbException {
    try {
      if (config.isTEMPLATE() && templateInit.compareAndSet(false, true)) {
        Template template = null;
        if (config.isTEMPLATE() && !schemaList.isEmpty()) {
          template = createTemplate(schemaList.get(0));
        }
        int sessionIndex = random.nextInt(sessionListMap.size());
        Session templateSession = new ArrayList<>(sessionListMap.keySet()).get(sessionIndex);
        registerTemplate(templateSession, template);
      }
      templateBarrier.await();
      for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
        registerDatabases(pair.getKey(), pair.getValue());
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
      throws IoTDBConnectionException, IOException, TsdbException {
    try {
      metaSession.createSchemaTemplate(template);
    } catch (StatementExecutionException e) {
      // do nothing
      handleRegisterException(e);
    }
  }

  @Override
  public void registerDatabases(Session metaSession, List<TimeseriesSchema> schemaList)
      throws TsdbException {
    // get all database
    Set<String> databaseNames = getAllDataBase(schemaList);
    // register database
    for (String databaseName : databaseNames) {
      try {
        metaSession.setStorageGroup(ROOT_SERIES_NAME + "." + databaseName);
        if (config.isTEMPLATE()) {
          metaSession.setSchemaTemplate(
              config.getTEMPLATE_NAME(), ROOT_SERIES_NAME + "." + databaseName);
        }
      } catch (Exception e) {
        handleRegisterException(e);
      }
    }
  }

  private void activateTemplate(Session metaSession, List<TimeseriesSchema> schemaList) {
    try {
      List<String> devicePaths =
          schemaList.stream()
              .map(schema -> ROOT_SERIES_NAME + "." + schema.getDeviceSchema().getDevicePath())
              .collect(Collectors.toList());
      metaSession.createTimeseriesUsingSchemaTemplate(devicePaths);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  private void registerTimeSeries(Session metaSession, List<TimeseriesSchema> timeseriesSchemas)
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

  // region select

  @Override
  public String selectTimeColumnIfNecessary() {
    return "";
  }

  @Override
  public String addFromClause(List<DeviceSchema> devices, StringBuilder builder) {
    builder.append(" FROM ").append(IoTDB.getDevicePath(devices.get(0)));
    for (int i = 1; i < devices.size(); i++) {
      builder.append(", ").append(IoTDB.getDevicePath(devices.get(i)));
    }
    return builder.toString();
  }

  @Override
  public String addDeviceIDColumnIfNecessary(List<DeviceSchema> deviceSchemas, String sql) {
    // do nothing
    return sql;
  }

  @Override
  public void addVerificationQueryWhereClause(
      StringBuffer sql,
      List<Record> records,
      Map<Long, List<Object>> recordMap,
      DeviceSchema deviceSchema) {
    sql.append(" WHERE time = ").append(records.get(0).getTimestamp());
    recordMap.put(records.get(0).getTimestamp(), records.get(0).getRecordDataValue());
    for (int i = 1; i < records.size(); i++) {
      Record record = records.get(i);
      sql.append(" or time = ").append(record.getTimestamp());
      recordMap.put(record.getTimestamp(), record.getRecordDataValue());
    }
  }

  @Override
  public void getValueFilterClause(
      List<DeviceSchema> deviceSchemas, int valueThreshold, StringBuilder builder) {
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        builder
            .append(" AND ")
            .append(IoTDB.getDevicePath(deviceSchema))
            .append(".")
            .append(sensor.getName())
            .append(" > ");
        if (sensor.getSensorType() == SensorType.DATE) {
          builder.append("'").append(LocalDate.ofEpochDay(Math.abs(valueThreshold))).append("'");
        } else {
          builder.append(valueThreshold);
        }
      }
    }
  }

  @Override
  public long getTimestamp(RowRecord rowRecord) {
    return rowRecord.getTimestamp();
  }

  @Override
  public int getQueryOffset() {
    return queryBaseOffset;
  }

  // TODO 用 count
  @Override
  public String getTotalLineNumberSql(DeviceSchema deviceSchema) {
    return "select * from " + IoTDB.getDevicePath(deviceSchema);
  }

  @Override
  public String getMaxTimeStampSql(DeviceSchema deviceSchema) {
    return "select * from " + IoTDB.getDevicePath(deviceSchema) + " order by time desc limit 1";
  }

  @Override
  public String getMinTimeStampSql(DeviceSchema deviceSchema) {
    return "select * from " + IoTDB.getDevicePath(deviceSchema) + " order by time limit 1";
  }

  // endregion

  // region insert
  @Override
  public Tablet createTablet(
      String insertTargetName,
      List<IMeasurementSchema> schemas,
      List<Tablet.ColumnType> columnTypes,
      int maxRowNumber) {
    return new Tablet(insertTargetName, schemas, maxRowNumber);
  }

  @Override
  public String getInsertTargetName(DeviceSchema schema) {
    return IoTDB.getDevicePath(schema);
  }

  @Override
  public void addIDColumnIfNecessary(
      List<Tablet.ColumnType> columnTypes, List<Sensor> sensors, IBatch batch) {
    // do nothing
  }

  @Override
  public void deleteIDColumnIfNecessary(
      List<Tablet.ColumnType> columnTypes, List<Sensor> sensors, IBatch batch) {
    // delete the value of the identity column to the value of each record
    for (int i = 0; i < batch.getRecords().size(); i++) {
      List<Object> dataValue = batch.getRecords().get(i).getRecordDataValue();
      dataValue.remove(batch.getDeviceSchema().getDevice());
      for (String key : batch.getDeviceSchema().getTags().keySet()) {
        dataValue.remove(batch.getDeviceSchema().getTags().get(key));
      }
    }
  }

  @Override
  public void sessionInsertImpl(Session session, Tablet tablet, DeviceSchema deviceSchema)
      throws IoTDBConnectionException, StatementExecutionException {
    if (config.isVECTOR()) {
      session.insertAlignedTablet(tablet);
    } else {
      session.insertTablet(tablet);
    }
  }

  @Override
  public void sessionCleanupImpl(Session session) {
    try {
      session.executeNonQueryStatement(
          "drop database root." + config.getDbConfig().getDB_NAME() + ".**");
      session.executeNonQueryStatement("drop schema template " + config.getTEMPLATE_NAME());
    } catch (IoTDBConnectionException e) {
      LOGGER.warn("Failed to connect to IoTDB:{}", e.getMessage());
    } catch (StatementExecutionException e) {
      LOGGER.warn("Failed to execute statement:{}", e.getMessage());
    }
  }

  // endregion

  @Override
  public Logger getLogger() {
    return LOGGER;
  }
}
