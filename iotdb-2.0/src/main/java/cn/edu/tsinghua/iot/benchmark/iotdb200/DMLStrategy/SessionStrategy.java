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

package cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.constant.ThreadName;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SQLDialect;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.exception.OperationFailException;
import cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy.SessionPool.AbstractSessionPool;
import cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy.SessionPool.TableSessionPoolWrapper;
import cn.edu.tsinghua.iot.benchmark.iotdb200.DMLStrategy.SessionPool.TreeSessionPoolWrapper;
import cn.edu.tsinghua.iot.benchmark.iotdb200.IoTDB;
import cn.edu.tsinghua.iot.benchmark.iotdb200.utils.IoTDBUtils;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBInsertMode;
import cn.edu.tsinghua.iot.benchmark.utils.NamedThreadFactory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SessionStrategy extends DMLStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionStrategy.class);
  static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Map<String, Binary> binaryCache =
      new ConcurrentHashMap<>(config.getWORKLOAD_BUFFER_SIZE(), 1.00f);
  private static final Map<SQLDialect, AbstractSessionPool> sessionPoolMap =
      new ConcurrentHashMap<>();
  private final IoTDB iotdb;
  private final AbstractSessionPool sessionPool;
  public static int numberS = 0;
  private static AtomicInteger activeThreadsUseSessionPool = new AtomicInteger(0);

  public SessionStrategy(DBConfig dbConfig, IoTDB iotdb, Integer number)
      throws IoTDBConnectionException {
    super(dbConfig);
    this.iotdb = iotdb;
    activeThreadsUseSessionPool.getAndIncrement();
    sessionPool = getSessionPool(dbConfig, number);
  }

  public static AbstractSessionPool getSessionPool(DBConfig dbConfig, int number) {
    SQLDialect sqlDialect = config.getIoTDB_DIALECT_MODE();
    return sessionPoolMap.computeIfAbsent(
        sqlDialect,
        dialect ->
            dialect == SQLDialect.TREE
                ? new TreeSessionPoolWrapper(dbConfig, number)
                : new TableSessionPoolWrapper(dbConfig, number));
  }

  @Override
  public Status insertOneBatch(IBatch batch, String devicePath) {
    DBInsertMode insertMode = dbConfig.getDB_SWITCH().getInsertMode();
    switch (insertMode) {
      case INSERT_USE_SESSION_TABLET:
        return insertOneBatchByTablet(batch);
      case INSERT_USE_SESSION_RECORD:
        return insertOneBatchByRecord(batch, devicePath);
      case INSERT_USE_SESSION_RECORDS:
        return insertOneBatchByRecords(batch, devicePath);
      default:
        throw new IllegalStateException("Unexpected INSERT_MODE value: " + insertMode);
    }
  }

  private Status insertOneBatchByTablet(IBatch batch) {
    Tablet tablet = genTablet(batch);
    task =
        service.submit(
            () -> {
              try {
                sessionPool.insertTablet(tablet, batch.getDeviceSchema());
              } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new OperationFailException(e);
              }
            });
    return waitWriteTaskToFinishAndGetStatus();
  }

  private Tablet genTablet(IBatch batch) {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<Tablet.ColumnCategory> columnTypes = new ArrayList<>();
    List<Sensor> sensors = batch.getDeviceSchema().getSensors();
    if (config.isIS_DOUBLE_WRITE()) {
      iotdb.deleteIDColumnIfNecessary(columnTypes, sensors, batch);
    }
    iotdb.addIDColumnIfNecessary(columnTypes, sensors, batch);
    int sensorIndex = 0;
    for (Sensor sensor : sensors) {
      SensorType dataSensorType = sensor.getSensorType();
      schemaList.add(
          new MeasurementSchema(
              sensor.getName(),
              Enum.valueOf(TSDataType.class, dataSensorType.name),
              Enum.valueOf(
                  TSEncoding.class,
                  Objects.requireNonNull(IoTDB.getEncodingType(dataSensorType)))));
      sensorIndex++;
    }
    String deviceId = iotdb.getInsertTargetName(batch.getDeviceSchema());
    Tablet tablet =
        iotdb.createTablet(
            deviceId,
            schemaList,
            columnTypes,
            batch.getRecords().size() * config.getDEVICE_NUM_PER_WRITE());
    int stepOff = 0;
    batch.reset();
    // Convert multiple batches to tablets
    for (int loop = 0; loop < config.getDEVICE_NUM_PER_WRITE(); loop++) {
      for (int recordIndex = stepOff;
          recordIndex < (batch.getRecords().size() + stepOff);
          recordIndex++) {
        Record record = batch.getRecords().get(recordIndex % batch.getRecords().size());
        sensorIndex = 0;
        long currentTime = record.getTimestamp();
        tablet.addTimestamp(recordIndex, currentTime);
        for (int recordValueIndex = 0;
            recordValueIndex < record.getRecordDataValue().size();
            recordValueIndex++) {
          switch (sensors.get(sensorIndex).getSensorType()) {
            case BOOLEAN:
              tablet.addValue(
                  recordIndex,
                  recordValueIndex,
                  (boolean) record.getRecordDataValue().get(recordValueIndex));
              break;
            case INT32:
              tablet.addValue(
                  recordIndex,
                  recordValueIndex,
                  (int) record.getRecordDataValue().get(recordValueIndex));
              break;
            case INT64:
            case TIMESTAMP:
              tablet.addValue(
                  recordIndex,
                  recordValueIndex,
                  (long) record.getRecordDataValue().get(recordValueIndex));
              break;
            case FLOAT:
              tablet.addValue(
                  recordIndex,
                  recordValueIndex,
                  (float) record.getRecordDataValue().get(recordValueIndex));
              break;
            case DOUBLE:
              tablet.addValue(
                  recordIndex,
                  recordValueIndex,
                  (double) record.getRecordDataValue().get(recordValueIndex));
              break;
            case TEXT:
            case STRING:
            case BLOB:
              tablet.addValue(
                  recordIndex,
                  recordValueIndex,
                  binaryCache
                      .computeIfAbsent(
                          (String) record.getRecordDataValue().get(recordValueIndex),
                          BytesUtils::valueOf)
                      .getValues());
              break;
            case DATE:
              tablet.addValue(
                  recordIndex,
                  recordValueIndex,
                  (LocalDate) record.getRecordDataValue().get(recordValueIndex));
              break;
            default:
              LOGGER.error("Unsupported Type: {}", sensors.get(sensorIndex).getSensorType());
          }
          sensorIndex++;
        }
      }
      if (batch.hasNext()) {
        batch.next();
      }
      stepOff += batch.getRecords().size();
    }
    return tablet;
  }

  private Status insertOneBatchByRecord(IBatch batch, String deviceId) {
    int failRecord = 0;
    List<String> sensors =
        batch.getDeviceSchema().getSensors().stream()
            .map(Sensor::getName)
            .collect(Collectors.toList());
    for (Record record : batch.getRecords()) {
      long timestamp = record.getTimestamp();
      List<TSDataType> dataTypes =
          IoTDBUtils.constructDataTypes(
              batch.getDeviceSchema().getSensors(), record.getRecordDataValue().size());
      List<Object> recordDataValue = convertTypeForBLOB(record, dataTypes);
      try {
        sessionPool.insertRecord(deviceId, timestamp, sensors, dataTypes, recordDataValue);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        failRecord++;
      }
    }
    if (failRecord == 0) {
      return new Status(true);
    } else {
      Exception e = new Exception("failRecord number is " + failRecord);
      return new Status(false, 0, e, e.toString());
    }
  }

  private Status insertOneBatchByRecords(IBatch batch, String deviceId) {
    List<String> deviceIds = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<String> sensors =
        batch.getDeviceSchema().getSensors().stream()
            .map(Sensor::getName)
            .collect(Collectors.toList());
    while (true) {
      for (Record record : batch.getRecords()) {
        deviceIds.add(deviceId);
        times.add(record.getTimestamp());
        measurementsList.add(sensors);
        List<TSDataType> dataTypes =
            IoTDBUtils.constructDataTypes(
                batch.getDeviceSchema().getSensors(), record.getRecordDataValue().size());
        valuesList.add(convertTypeForBLOB(record, dataTypes));
        typesList.add(dataTypes);
      }
      if (!batch.hasNext()) {
        break;
      }
      batch.next();
      // Switch to the device in the next batch  e.g. root.group_1.d_1
      deviceId = IoTDBUtils.getDevicePath(batch.getDeviceSchema(), IoTDB.ROOT_SERIES_NAME);
    }
    task =
        service.submit(
            () -> {
              try {
                sessionPool.insertRecords(
                    deviceIds, times, measurementsList, typesList, valuesList);
              } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new OperationFailException(e);
              }
            });
    return waitWriteTaskToFinishAndGetStatus();
  }

  private List<Object> convertTypeForBLOB(Record record, List<TSDataType> dataTypes) {
    // String change to Binary
    List<Object> dataValue = record.getRecordDataValue();
    for (int recordValueIndex = 0;
        recordValueIndex < record.getRecordDataValue().size();
        recordValueIndex++) {
      if (Objects.requireNonNull(dataTypes.get(recordValueIndex)) == TSDataType.BLOB) {
        dataValue.set(
            recordValueIndex,
            binaryCache.computeIfAbsent(
                (String) record.getRecordDataValue().get(recordValueIndex), BytesUtils::valueOf));
      }
    }
    return dataValue;
  }

  @Override
  public long executeQueryAndGetStatusImpl(
      String executeSQL, Operation operation, AtomicBoolean isOk, List<List<Object>> records) {
    AtomicLong queryResultPointNum = new AtomicLong();
    AtomicInteger line = new AtomicInteger();
    task =
        service.submit(
            () -> {
              try {
                SessionDataSet sessionDataSet = sessionPool.executeQueryStatement(executeSQL);
                if (config.isIS_COMPARISON()) {
                  while (sessionDataSet.hasNext()) {
                    RowRecord rowRecord = sessionDataSet.next();
                    line.getAndIncrement();
                    List<Object> record = new ArrayList<>();
                    switch (operation) {
                      case AGG_RANGE_QUERY:
                      case AGG_VALUE_QUERY:
                      case AGG_RANGE_VALUE_QUERY:
                        break;
                      default:
                        record.add(rowRecord.getTimestamp());
                        break;
                    }
                    List<Field> fields = rowRecord.getFields();
                    for (int i = 0; i < fields.size(); i++) {
                      switch (operation) {
                        case LATEST_POINT_QUERY:
                          if (i == 0 || i == 2) {
                            continue;
                          }
                        default:
                          break;
                      }
                      record.add(fields.get(i).toString());
                    }
                    records.add(record);
                  }
                } else {
                  SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
                  while (iterator.next()) {
                    line.getAndIncrement();
                  }
                }
                sessionDataSet.close();
              } catch (StatementExecutionException | IoTDBConnectionException e) {
                LOGGER.error("exception occurred when execute query={}", executeSQL, e);
                isOk.set(false);
              }
              long resultPointNum = line.get();

              if (config.getIoTDB_DIALECT_MODE().equals(SQLDialect.TABLE)) {
                resultPointNum *= config.getQUERY_SENSOR_NUM();
              } else {
                if (!Operation.LATEST_POINT_QUERY.equals(operation)) {
                  resultPointNum *= config.getQUERY_DEVICE_NUM();
                  resultPointNum *= config.getQUERY_SENSOR_NUM();
                }
              }
              queryResultPointNum.set(resultPointNum);
            });
    try {
      task.get(config.getREAD_OPERATION_TIMEOUT_MS(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      task.cancel(true);
      return queryResultPointNum.get();
    }
    return queryResultPointNum.get();
  }

  @Override
  public List<Integer> verificationQueryImpl(String sql, Map<Long, List<Object>> recordMap)
      throws IoTDBConnectionException, StatementExecutionException {
    int point = 0, line = 0;
    try (SessionDataSet sessionDataSet = sessionPool.executeQueryStatement(sql)) {
      while (sessionDataSet.hasNext()) {
        RowRecord rowRecord = sessionDataSet.next();
        // The table model and the tree model obtain time differently
        long timeStamp = iotdb.getTimestamp(rowRecord);
        List<Object> values = recordMap.get(timeStamp);
        for (int i = 0; i < values.size(); i++) {
          String value = iotdb.getValue(rowRecord, i);
          String target = String.valueOf(values.get(i));
          if (!value.equals(target)) {
            LOGGER.error("Using SQL: " + sql + ",Expected:" + value + " but was: " + target);
          } else {
            point++;
          }
        }
        line++;
      }
    }
    return Arrays.asList(point, line);
  }

  @Override
  public List<List<Object>> deviceQueryImpl(String sql) throws Exception {
    List<List<Object>> result = new ArrayList<>();
    try (SessionDataSet sessionDataSet = sessionPool.executeQueryStatement(sql)) {
      while (sessionDataSet.hasNext()) {
        List<Object> line = new ArrayList<>();
        RowRecord rowRecord = sessionDataSet.next();
        line.add(rowRecord.getTimestamp());
        List<Field> fields = rowRecord.getFields();
        for (Field field : fields) {
          line.add(field.getStringValue());
        }
        result.add(line);
      }
    }
    return result;
  }

  @Override
  public DeviceSummary deviceSummary(
      String device, String totalLineNumberSql, String maxTimestampSql, String minTimestampSql)
      throws TsdbException {
    int totalLineNumber = 0;
    long minTimeStamp, maxTimeStamp;
    try {
      SessionDataSet sessionDataSet = sessionPool.executeQueryStatement(totalLineNumberSql);
      while (sessionDataSet.hasNext()) {
        sessionDataSet.next();
        totalLineNumber++;
      }
      sessionDataSet.close();

      sessionDataSet = sessionPool.executeQueryStatement(maxTimestampSql);
      RowRecord rowRecord = sessionDataSet.next();
      maxTimeStamp = iotdb.getTimestamp(rowRecord);
      sessionDataSet.close();

      sessionDataSet = sessionPool.executeQueryStatement(minTimestampSql);
      rowRecord = sessionDataSet.next();
      minTimeStamp = iotdb.getTimestamp(rowRecord);
      sessionDataSet.close();
    } catch (IoTDBConnectionException e) {
      throw new TsdbException("Failed to connect to IoTDB:" + e.getMessage());
    } catch (StatementExecutionException e) {
      throw new TsdbException("Failed to execute statement:" + e.getMessage());
    }
    return new DeviceSummary(device, totalLineNumber, minTimeStamp, maxTimeStamp);
  }

  Status waitWriteTaskToFinishAndGetStatus() {
    try {
      task.get(config.getWRITE_OPERATION_TIMEOUT_MS(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      task.cancel(true);
      LOGGER.error("insertion failed", e);
      return new Status(false, 0, e, e.toString());
    }
    return new Status(true);
  }

  @Override
  public void init() {
    // TODO clean 数据、SchemaClient、DataClient 都用到了这里，因此名字写死不合适。
    this.service =
        Executors.newSingleThreadExecutor(
            new NamedThreadFactory(ThreadName.DATA_CLIENT_EXECUTE_JOB.getName()));
  }

  @Override
  public void cleanup() {
    try {
      iotdb.sessionCleanupImpl(sessionPool);
    } catch (IoTDBConnectionException e) {
      LOGGER.warn("Failed to connect to IoTDB:" + e.getMessage());
    } catch (StatementExecutionException e) {
      LOGGER.warn("Failed to execute statement:" + e.getMessage());
    }
  }

  @Override
  public void close() throws TsdbException {
    activeThreadsUseSessionPool.getAndDecrement();
    try {
      if (sessionPool != null && activeThreadsUseSessionPool.get() == 0) {
        sessionPoolMap.remove(config.getIoTDB_DIALECT_MODE());
        sessionPool.close();
      }
    } finally {
      service.shutdown();
    }
  }
}
